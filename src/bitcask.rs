//
// Module that implements a "Bitcask" style Key/Value store.
//
pub mod bitcask {
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::vec::Vec;
    use std::fs::read_dir;
    use std::fs::File;
    use threadpool::ThreadPool;
    use std::sync::mpsc::channel;
    use std::sync::RwLock;
    use std::io;
    use crc32fast;

    // A simple monotonically increasing integer that identifies each datafile or hintsfile.
    // We use it as the basename of each such file.  We track the higest basename we've seen and
    // just add 1 when we need a new datafile or corresponding hints file.
    type BitcaskFileID = i32;

    //
    // Define the format of the in-memory database of keys and which data file contains their values.
    // NOTE: this is NOT an on-disk value, it can be changed without affecting data retention
    //
    struct BitcaskKeymapEntry {
	value_size: i32,			// The size of the value of that key
	fileid: BitcaskFileID,			// Which datafile contains that K/V pair
	offset: i64,				// The byte offset of that K/V pair within that datafile
    }
    impl BitcaskKeymapEntry {
	pub fn new(value_size: i32, fileid: BitcaskFileID, offset: i64) -> Box<BitcaskKeymapEntry> {
	    Box::new(BitcaskKeymapEntry {
		value_size: value_size,
		fileid: fileid,
		offset: offset,
	    })
	}
    }

    // We need to know the type of operation in the on-disk records of the data files.
    // NOTE: this IS an on-disk value, it cannot be changed without affecting data retention
    enum BitcaskDatafileRectype {
	PUT,
	DELETE,
    }

    //
    // Define the format and operations on one of the data files used by Bitcask.
    // NOTE: this IS an on-disk value, it cannot be changed without affecting data retention
    //
    struct BitcaskDatafileRecord {
	crc: i32,
	key: i32,
	op: BitcaskDatafileRectype,
	value_size: i32,			// This is the actual number of bytes in the value, but the total size of this record 
						// will be SILENTLY rounded up to the next multiple of 4.
	value: [u8; 4096],			// This is a maximally-sized structure, it will be shrunk-to-fit when on-disk
						// (modulo the rounding up).  NOTE: this fixed-size is an ugly limitation but one that
						// I need to live with until I figure out how to do variable sized buffers in rust
    }
    impl BitcaskDatafileRecord {
	pub fn new(key: i32, op: BitcaskDatafileRectype, value_size: i32, value: String) -> Box<BitcaskDatafileRecord> {
	    Box::new(BitcaskDatafileRecord{
		key: key,			// Copy the key into place
		op: op,				// Copy the operation into place (PUT or DELETE)
		value_size: value_size,		// The actual number of valid bytes in the value
		value = value,			// GROT - Copy the string into place
		crc: crc32fast::hash(rec),	// GROT - Need way to limit the "byte slice" to just the valid bytes
	    })
	}
    }

    //
    // Track all the extant data files.
    //
    struct BitcaskDatafile {
	name: String,				// The relative pathname of the data file
	ID: BitcaskFileID,			// What "number" is it?
	file_lock: RwLock<File>,		// Protects the File structure to ensure seeks() go with reads()/writes()
    }
    impl BitcaskDatafile {
	//
	// Create a new data file.
	//
	pub fn new(dirpath: String, ID: BitcaskFileID) -> Result<Box<BitcaskDatafile>,io::Error> {
	    let filename = format("{}/{}.data", dirpath, ID+1);
	    Ok(Box::new(BitcaskDatafile {
		name: filename.clone(),
		ID: ID + 1,
		file: File::create(&filename)?,
	    }))
	}

	//
	// Open an existing data file.
	//
	pub fn open(dirpath: String, ID: BitcaskFileID) -> Result<Box<BitcaskDatafile>,io::Error> {
	    let filename = format("{}/{}.data", dirpath, ID);
	    Ok(Box::new(BitcaskDatafile {
		name: filename.clone(),
		ID: ID,
		file: File::open(&filename)?,
	    }))
	}

	//
	// Read a BitcaskDatafileRecord from the given offset in the data file.
	//
	pub fn get(self: Self, offset: i64) -> Result<Box<BitcaskKeymapEntry>,Self::Error> {
	    let mut rec = Box<BitcaskDatafileRecord;			// Max sized, will read more than what we really want
	    {
		let f = self.file_lock.write().unwrap();		// Protect the data structure while we do our seek and read
		f.seek(SeekFrom::Start(offset))?;
		let bytecount = f.read(&rec)?;
	    }								// Drop the reader lock
	    Ok(rec)
	}

	//
	// Create a BitcaskDatafileRecord for a new KV, append it to the datafile, and optionally flush it out.
	//
	pub fn put(self: Self, key: i32, value: &String, flush: bool) -> Result<i64,Self::Error> {
	    let mut rec = BitcaskDatafileRecord::new(key, BitcaskDatafileRectype::PUT, i32::try_from(value.len())?, value);
	    let offset: i64 = 0;					// Establish scope outside the lock hold region
	    {
		let f = self.file_lock.write().unwrap();		// Protect the data structure while we do our seek and write
		let offset = f.seek(SeekFrom::End(0))?;			// Capture the offset of this new record
		let bytecount = f.write(&rec)?;
	    }								// Drop the reader lock
	    if flush == true {						// Happens outside the lock
		Self::sync(self);					// Ensure on-disk stability, if requested
	    }
	    Ok(offset)
	}

	//
	// Create a BitcaskDatafileRecord for deleting a KV, append it to the datafile, and optionally flush it out.
	//
	pub fn delete(self: Self, key: i32, flush: bool) -> Result<i64,io::Error> {
	    let mut rec = BitcaskDatafileRecord::new(key, BitcaskDatafileRectype::DELETE, 0, &String::new());
	    let offset: i64 = 0;					// Establish scope outside the lock hold region
	    {
		let f = self.file_lock.write().unwrap();		// Protect the data structure while we do our seek and write
		let offset = f.seek(SeekFrom::End(0))?;			// Capture the offset of this new record
		let bytecount = f.write(&rec)?;
	    }								// Drop the reader lock
	    if flush == true {						// Happens outside the lock
		Self::sync(self);					// Ensure on-disk stability, if requested
	    }
	    Ok(offset)
	}

	//
	// Flush out any cached bytes for a datafile
	//
	pub fn sync(self: Self) -> Result<bool,io::Error>  {
	    self.file.sync_all()?;
	    Ok(true)
	}
    }

    //
    // TODO: Need a full-on utility class for hints files.
    // This class is not persistent, a hint file is either read at boot time and then forgotten,
    // or it is generated from a data file without regard to anything else going on in the system.
    //
    struct BitcaskHintsfile {
	name: String,				// The relative pathname of the hints file
	ID: BitcaskFileID,			// What "number" is it?
	file_lock: RwLock<File>,		// Protects the File structure to ensure seeks() go with reads()/writes()
    }
    impl BitcaskHintsfile {
	//
	// Create a new hints file.
	//
	pub fn new(dirpath: String, ID: BitcaskFileID) -> Result<Box<BitcaskHintsfile>,io::Error> {
	    let filename = format("{}/{}.data", dirpath, ID+1);
	    Ok(Box::new(BitcaskHintsfile {
		name: filename.clone(),
		ID: ID,
		file: File::create(&filename)?,
	    }))
	}
    
	//
	// Define the format and operations on one of the hint files used by Bitcask.
	// This file is a very quick way to repopulate the in-memroy keymap structure.
	//
	struct BitcaskHintsfileRecord {
	    key: i32,				// The key of a KV we're storing
	    op: BitcaskDatafileRectype,		// Is this a PUT or a DELETE?
	    value_size: i32,			// The size of the value for that KV
	    offset: i64,				// the offset within the data file where that KV is stored
	}
	impl BitcaskHintsfileRecord {
	    pub fn new(key: i32, op: BitcaskDatafileRectype, value_size: i32, value: String, offset: i64) -> Box<BitcaskHintsfileRecord> {
		Box::new(BitcaskHintsfileRecord{
		    key: key,			// Copy the key into place
		    op: op,				// Copy the operation into place (PUT or DELETE)
		    value_size: value_size,		// The actual number of valid bytes in the value
		    offset: offset,			// the offset within the file of that record for that key
		}) 
	    }
	}

	// Generate a hint file by sumarizing all the operations in the data file by recording the *surviving* PUT and DELETE operations.
	// Read through the datafile, recording each op (and its  key and the byte offset of the record) into an in-memory HashMap.
	// If this is a DELETE, remove any existing PUTs for from the hint summary that key and record the DELETE in the hint summary.
	// If this is a PUT, remove any existing DELETEs or PUTs for that key from the hint summary, record the new PUT key and byte offset.
	// The hints file is 'datafile.name' with ".data" changesd to ".hints".
	pub fn hintsfile_generate(datafile: &BitcaskDatafile) -> Result<bool,io::Error> {
	    // We use Self::appendrec again to get the compact binary representation of the op, key, and possibly value.
	    // offset = appendrec(BitcaskDatafileRectype::PUT, key, value>)?;
	    Ok(true)
	}

	// Read all the "*.hints" files into the in-memory keymap structure.
	// The saved_hintQ must be in sorted order so that DELETE records that follow PUT record in
	// time will make the key go away, if they were not processed in order keys would stick 
	// around after they were deleted.
	pub fn hintsfile_import(keymap: &HashMap<i32, BitcaskKeymapEntry>, filename: String) -> Result<bool,io::Error> {
	    Ok(true)
	}

	//
	// This is a work routine and is lower priority than some other work, so queuing it up for later.
	// There's very likely a much simpler way to identify this fill-in-the-missing-file task,
	// but I don't have time to google for good library solutions right now.
	// This would need to run during single-threaded mode either startup or shutdown processing.
	// If I separated out the "read them in" part, then this could happen in a separate thread while
	// normal processing was going on, but we'd need to collect the lists of files in one thread
	// and then fork off separate thread(s?) to "generate" each hints file, and retain the list of existing
	// hints files in the master thread so we could read them in and populate the in-memory keymap.
	//
	// This is the core data-resiliency routine.  It recovers from crashes and outages by
	// depending upon the log-structure of the data files.  It generates any missing "*.hints"
	// files so that the next crash/reboot will start faster.  Any existing, partially complete,
	// data file becomes a read-only part of the dataset until merge time.
	//
	pub fn hintsfile_find_missing_files(cask: &Bitcask, dirpath: &String) -> Result<bool,std::io::Error> {
	    //
	    // Identify all existing "*.data" and ".*hints" files in the database directory
	    //
	    let mut dataQ: VecDeque::<String> = VecDeque::<String>::new();		// Set up parallel queues
	    let mut hintQ: VecDeque::<String> = VecDeque::<String>::new();
	    for entry in read_dir(dirpath.clone())? {
		let entry = entry?;
		let filename = entry.file_name().to_string_lossy().to_string();		// The OsString type is difficult to work with
		if entry.metadata()?.is_file() {
		    if filename.ends_with(".data") {
			dataQ.push_back(filename);
		    } else if filename.ends_with(".hints") {
			hintQ.push_back(filename);
		    }
		}
	    }
	    Ok(true)
	}
    }

    //
    // The core logic that ties the Bitcask components together.
    //
    #[derive(Default)]
    pub struct Bitcask {
	keymap: RwLock<HashMap<i32, BitcaskKeymapEntry>>,		// Protects the in-memory HashMap of all extant KV pairs
	current: RwLock<BitcaskDatafile>,				// Protects changes to the 'current' field (not the datafile itself)
	datafiles: RwLock<HashMap<BitcaskFileID, BitcaskDatafile>>,	// Protects all the map of the archived data files
	dirpath: String,						// The directory storing everything
	maxID: BitcaskFileID,						// The highest ID of an existing datafile
    }
    impl Bitcask {
	//
	// Create a new Bitcask and (re)fill it by (generating and) reading all hintfiles in the database directory.
	//
	pub fn new(dirpath: &String) -> Result<Box<Bitcask>,io::Error> {
	    let cask = Box::new(Bitcask { });
	    cask.keymap = RwLock::new(HashMap::new());
	    cask.datafiles = RwLock::new(HashMap::new());
	    cask.current = RwLock::new(BitcaskDatafile::new(dirpath, ID)?;
	    cask.dirpath = dirpath.clone();
	    //hintsfile_find_missing_files(&cask, dirpath)?;
	    //hintsfile_import(keymap: &HashMap<i32, BitcaskKeymapEntry>, filename: String)?;
	    Ok(cask)
	}

	// 
	pub fn get(self: &Self, key: i32) -> Result<String,io::Error> {
	    let entry = 0;						// Establish scope outside the lock hold region
	    {
		let map = self.keymap.read().unwrap();			// Protect the data structure while we do our lookup
		map.get(key, entry)?;					// Get the KV location from the index
	    }								// Drop the reader lock
	    let value = 0;						// Establish scope outside the lock hold region
	    {
		let df = self.current.read().unwrap();			// Protect changes to 'current' while we do our lookup
		let value = df.get(key, value, true)?;			// Get the KV from the datafile location
	    }								// Drop the reader lock
	    Ok(value)
	}

	//
	// Insert a new KV or update an existing KV
	//
	pub fn put(self: &Self, key: i32, value: &String) -> Result<bool,io::Error> {
	    {
		let df = self.current.read().unwrap();			// Protect changes to 'current' while we do our lookup
		let entry = df.put(key, value, true)?;			// Append a PUT record
	    }								// Drop the reader lock
	    Ok(true)
	}

	//
	// Delete a (potentially) existing KV
	// 
	pub fn delete(self: &Self, key: i32) -> Result<bool,io::Error>  {
	    {
		let df = self.current.read().unwrap();			// Protect changes to 'current' while we do our delete
		let entry = df.delete(key, true)?;			// Append a DELETE record
	    }								// Drop the reader lock
	    {
		let map = self.keymap.write().unwrap();			// Protect the data structure while we do our insert
		map.remove(&key)?;					// Remove it from the index
	    }								// Drop the writer lock
	    Ok(true)
	}

	//
	// Return a Vec<i32> containing all the keys in the database
	// 
	pub fn list_keys(self: Self) -> Box<Vec<i32>> {
	    let keyvec = Box::<Vec::<i32>>;				// Establish scope outside the lock hold region
	    {
		let map = self.keymap.read().unwrap();			// Protect the data structure while we do our iterator
		for key in map.keys() {
		    keyvec.push(*key);
		}
	    }								// Drop the reader lock
	    keyvec
	}

	//
	// Sync out the currently open data file.
	// 
	pub fn sync(self: &Self) -> Result<bool,io::Error> {
	    {
		let df = self.current.read().unwrap();			// Protect changes to 'current' while we do our delete
		df.sync()?;
	    }								// Drop the reader lock
	    Ok(true)
	}

	//
	// Close the current datafile and start a new one.
	// We defer creating the hint files until shutdown or reboot, but we could fork a thread to do it if we wanted to.
	//
	pub fn rotate(self: &Self) -> Result<bool,io::Error> {
	    {
		let df = self.current.read().unwrap();			// Protect changes to 'current' while we do our rotation
		let map = self.datafiles.write().unwrap();		// Protect the data structure while we do our insert
		map.insert(self.current.ID, self.current)?;		// Move the current/closing datafile to the readonly archive
		let self.current = BitcaskDatafile::new(dirpath, ID)?;	// Create a new current datafile to write to
	    }								// Drop both of the locks
	    // Self::generate_hints_file(&cask, &self.dirpath, datafile: &String, hintfile: &String)?;

	    Ok(true)
	}

	//
	// Shutdown the whole system.
	//
	pub fn shutdown(self: &Self) -> Result<bool,io::Error> {
	    // TODO: join() all the threads
	    self.sync(Self)?;
	    // TODO: close all the data files
	    // Self::generate_hints_file(&cask, &self.dirpath, datafile: &String, hintfile: &String)?;
	}
    }
}
