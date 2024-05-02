//
// Module that implements a "Bitcask" style Key/Value store.
//
//
pub mod bitcask {
    use std::collections::HashMap;
    use std::collections::VecDeque;
    use std::vec::Vec;
    use std::fs::read_dir;
    use std::fs::File;
    use std::io;
    use crc32fast;

    // A simple monotonically increasing integer that identifies each datafile, in fact it's the basename of each such file.
    // We track the higest basename we've seen and juist add 1 when we need a new datafile.
    type BitcaskDatafileID = i32;

    //
    // Define the format of the in-memory database of keys and which data file contains their values.
    //
    struct BitcaskKeymapEntry {
	value_size: i32,			// The size of the value of that key
	fileid: BitcaskDatafileID,		// Which datafile contains that K/V pair
	offset: i64,				// The byte offset of that K/V pair within that datafile
    }
    impl BitcaskKeymapEntry {
	pub fn new(value_size: i32, fileid: BitcaskDatafileID, offset: i64) -> Box<BitcaskKeymapEntry> {
	    Box::new(BitcaskKeymapEntry {
		value_size: value_size,
		fileid: fileid,		// Which datafile contains that K/V pair
		offset: offset,
	    })
	}
    }

    // We need to know the type of operation in the on-disk records of the data files.
    enum BitcaskDatafileRectype {
	PUT,
	DELETE,
    }

    //
    // Define the format and operations on one of the data files used by Bitcask.
    //
    struct BitcaskDatafileRecord {
	crc: i32,
	key: i32,
	op: BitcaskDatafileRectype,
	value_size: i32,		// This is the actual number of bytes in the value, but the total size of this record 
					// will be SILENTLY rounded up to the next multiple of 4.
	value: [char; 4096],		// This is truncated to the next multiple of 4 over the actual length of the key
    }

    //
    // Track all the extant data files.
    //
    struct BitcaskDatafile {
	name: String,
	ID: BitcaskDatafileID,
	file: File,
	offset: i64,
    }
    impl BitcaskDatafile {

	// Create a record of an operation (a put or delete) and append it to the current datafile.
	fn appendrec(optype: BitcaskDatafileRectype, key: i32, value: &String) -> Result<i64,io::Error> {
	    //
	    // This work is pushed onto the queue and deferred until I get bigger fish all fried up.
	    // We need to pack the key andvalue intothe record's bytes, then generate the CRC.  We
	    // Also need to capture the byte offset within the file and keep it in the keymap record.
	    //
	    //    let record = 
	    //    record.type = optype;
	    //    record.key = key;
	    //    if value != None {
	    //        record.value = value;
	    //    } else {
	    //    }
	    let offset: i64 = 0;	// GROT - capture the real file offset
	    Ok(offset)
	}

	pub fn new(filename: String, ID: BitcaskDatafileID) -> Result<Box<BitcaskDatafile>,io::Error> {
	    Ok(Box::new(BitcaskDatafile {
		name: filename.clone(),
		ID: ID + 1,
		file: File::open(&filename)?,
		offset: 0,
	    }))
	}
	pub fn put(self: Self, key: i32, value: &String, flush: bool) -> Result<Box<BitcaskKeymapEntry>,Self::Error> {
	    let offset: i64 = Self::appendrec(BitcaskDatafileRectype::PUT, key, value)?;
	    if flush == true {
		Self::sync(self);
	    }
	    let valsize: i32 = i32::try_from(value.len())?;
	    Ok(BitcaskKeymapEntry::new(valsize, self.ID, self.offset))
	}
	pub fn delete(self: Self, key: i32, flush: bool) -> Result<i64,io::Error> {
	    let offset: i64 = 0;	// GROT - capture the real file offset
	    offset = Self::appendrec(BitcaskDatafileRectype::DELETE, key, &String::new())?;
	    if flush == true {
		Self::sync(self);
	    }
	    Ok(offset)
	}
	//pub fn list_keys(self: Self) -> [i32] {
	//    [12]
	//}
	pub fn sync(self: Self) -> Result<bool,io::Error>  {
	    self.file.sync_all()?;
	    Ok(true)
	}
    }
    
    //
    // Define the format and operations on one of the hint files used by Bitcask.
    // This file is a very quick way to repopulate the in-memroy keymap structure.
    //
    struct BitcaskHintsfileRecord {
	key: i32,				// The key of a KV we're storing
	value_size: i32,			// The size of the value for that KV
	offset: i64,				// the offset within the data file where that KV is stored
    }

    // Generate a hint file by sumarizing all the operations in the data file by recording the *surviving* PUT and DELETE operations.
    // Read through the datafile, recording each op (and its  key and the byte offset of the record) into an in-memory HashMap.
    // If this is a DELETE, remove any existing PUTs for from the hint summary that key and record the DELETE in the hint summary.
    // If this is a PUT, remove any existing DELETEs or PUTs for that key from the hint summary, record the new PUT key and byte offset.
    // The hints file is 'datafile.name' with ".data" changesd to ".hints".
    fn bitcask_hintsfile_generate(datafile: &BitcaskDatafile) -> Result<bool,io::Error> {
	// We use Self::appendrec again to get the compact binary representation of the op, key, and possibly value.
	// offset = appendrec(BitcaskDatafileRectype::PUT, key, value>)?;
	Ok(true)
    }

    // Read a "*.hints" file into the in-memory keymap structure. 
    fn bitcask_hintsfile_import(keymap: &HashMap<i32, BitcaskKeymapEntry>, filename: String) -> Result<bool,io::Error> {
	Ok(true)
    }

    //
    // The core logic that ties the Bitcask components together.
    //
    #[derive(Default)]
    pub struct Bitcask {
	keymap: HashMap<i32, BitcaskKeymapEntry>,
	current: BitcaskDatafile,
	datafiles: HashMap<BitcaskDatafileID, BitcaskDatafile>,
	dirpath: String,
    }
    impl Bitcask {
	//
	// Work routine, lower priority than some other work, so queuing it up for later.
	// There's very likely a much simpler way to identify this fill-in-the-missing task, but I don't have time to google for good
	// library solutions right now.
	//
	fn find_missing_hints_and_generate_them_and_read_them_in(cask: &Bitcask, dirpath: &String) -> Result<bool,std::io::Error> {
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

	    //
	    // Identify the missing "*.hints" files, panic if there's any missing "*.data" files.
	    // Generate any hint files that are missing.
	    //
	    dataQ.make_contiguous().sort();
	    hintQ.make_contiguous().sort();
	    let saved_hintQ = hintQ.clone();
	    let dname: Option<String> = None;
	    let hname: Option<String> = None;
	    //	    while true {
	    //		let tmp = dataQ.pop_front();
	    //		if tmp.ends_with(".data") {
	    //		} else if tmp.ends_with(".hints") {
	    //		}
	    //		if dname == None {
	    //		    let dname = dataQ.pop_front();
	    //		}
	    //		if hname == None {
	    //		    let hname = hintQ.pop_front();
	    //		}
	    //	    }
	    //	    while !dataQ.is_empty() && !hintQ.is_empty() {
	    //		let data = dataQ.pop_front();
	    //		let hint = hintQ.pop_front();
	    //		if data == hint {
	    //		} else {
	    //		}
	    //	    }
	    //
	    //	    if filename.ends_with(".data") {
	    //		let cask = BitcaskDatafile::new(filename)?;
	    //          bitcask_hintsfile_generate(datafile)?;
	    //	    } else if filename.ends_with(".hints") {
	    //		let hints = bitcask_hintsfile_import(&cask.keymap, filename)?;
	    //	    }

	    // Read all the "*.hints" files into the in-memory keymap structure. Read all the
	    // "*.hints" files into the in-memory keymap structure as long as we have a list of them.
	    // The saved_hintQ must be in sorted order so that DELETE records that follow PUT record in
	    // time will make the key go away, if they were not processed in order keys would stick 
	    // around after they were deleted.
	    for hname in saved_hintQ {
		bitcask_hintsfile_import(&cask.keymap, hname);
	    }

	    Ok(true)
	}

	//
	// Create a new Bitcask and fill it by reading any/all datafiles and hitfiles in the database directory.
	//
	pub fn new(dirpath: &String) -> Result<Box<Bitcask>,io::Error> {
	    let cask = Box::new(Bitcask { });
	    cask.keymap = HashMap::new();
	    cask.datafiles = HashMap::new();
	    cask.current = BitcaskDatafile::new(filename: String, ID: BitcaskDatafileID)?;
	    cask.dirpath = dirpath.clone();

	    // Identify the missing "*.hints" files, and panic if there's any missing "*.data" files.
	    // This is the core data-resiliency routine.  It recovers from crashed and outages by
	    // depending upon the log-structure of the data files.  It generates any missing "*.hints"
	    // files so that the next crash/reboot will start faster.  Any existing, partially complete,
	    // data file becomes a read-only part of the dataset until merge time.  Also, read all the
	    // "*.hints" files into the in-memory keymap structure as long as we have a list of them.
	    Self::find_missing_hints_and_generate_them_and_read_them_in(&cask, dirpath)?;

	    Ok(cask)
	}
	pub fn get(self: &Self, key: i32) -> Option<String> {
	    None
	}
	pub fn put(self: &Self, key: i32, value: &String) -> Result<bool,io::Error>  {
	    let entry = self.current.put(key, value, true)?;			// Append a PUT record
	    self.keymap.insert(key, entry)?;					// Insert it into the index
	    Ok(true)
	}
	pub fn delete(self: &Self, key: i32) -> Result<bool,io::Error>  {
	    self.current.delete(key, true)?;					// Append a DELETE record
	    self.keymap.remove(&key)?;						// Remove it from the index
	    Ok(true)
	}
	pub fn list_keys(self: Self) -> Box<Vec<i32>> {
	    let keyvec = Box::<Vec::<i32>>;
	    for key in self.keymap.keys() {
		keyvec.push(*key);
	    }
	    keyvec
	}
	pub fn sync(self: &Self) -> Result<bool,io::Error> {
	    self.current.sync()?;
	    Ok(true)
	}
	pub fn close(self: &Self) -> Result<bool,io::Error> {
	    self.datafiles.insert(self.current.ID, self.current)?;		// Move the current/closing datafile to the readonly archive
	    // generate_hints_file(&cask, &self.dirpath, datafile: &String, hintfile: &String)?;
	    // GROT - I need to fix up the datafile/hintsfile argument types between the callers of this routine.
	    let self.current = BitcaskDatafile::new(filename: String, ID: BitcaskDatafileID)?; // Create a new current datafile to write to
	    Ok(true)
	    // We defer creating the hint files
	}
    }
}
