//
// Module that implements a "Bitcask" style Key/Value store.
//
//
pub mod bitcask {
    use std::collections::HashMap;
    use std::fs::read_dir;
    use std::fs::File;
    use std::io;
    use crc32fast;

    //
    // Define the format and operations on one of the data files used by Bitcask.
    //
    struct BitcaskDatafileRecord {
	crc: i32,
	key: i32,
	value_size: i32,
	value: [char; 4096],
    }

    type BitcaskDatafileID = i32;

    struct BitcaskDatafile {
	name: String,
	file: File,
	offset: i64,
    }

    impl BitcaskDatafile {
	pub fn new(filename: String) -> Result<Box<BitcaskDatafile>,io::Error> {
	    Ok(Box::new(BitcaskDatafile {
		name: filename.clone(),
		file: File::open(&filename)?,
		offset: 0,
	    }))
	}
	pub fn upsert(self: Self, key: i32, value: String, sync: bool) -> Option<String> {
	    None
	}
	pub fn delete(self: Self, key: i32, sync: bool) {
	}
	//pub fn list_keys(self: Self) -> [i32] {
	//    [12]
	//}
	pub fn sync(self: Self) {
	}
	pub fn close(self: Self) {
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
    fn bitcask_hintsfile_generate(datafile: &BitcaskDatafile) -> Result<bool,io::Error> {
	Ok(true)
    }
    fn bitcask_hintsfile_import(keymap: &HashMap<i32, BitcaskKeymapEntry>, filename: String) -> Result<bool,io::Error> {
	Ok(true)
    }

    //
    // Define the format of the in-memory database of keys and which data file contains their values.
    //
    struct BitcaskKeymapEntry {
	value_size: i32,			// The size of the value of that key
	file: BitcaskDatafileID,		// Which datafile contains that K/V pair
	offset: i64,				// The byte offset of that K/V pair within that datafile
    }

    //
    // The core logic that ties the Bitcask component together.
    //
    pub struct Bitcask {
	datafiles: HashMap<BitcaskDatafileID, BitcaskDatafile>,
	keymap: HashMap<i32, BitcaskKeymapEntry>,
    }
    impl Bitcask {
	pub fn new(dirpath: String) -> Result<Box<Bitcask>,io::Error> {
	    let cask = Box::new(Bitcask {
		datafiles: HashMap::new(),
		keymap: HashMap::new(),
	    });
	    for entry in read_dir(dirpath)? {
		let entry = entry?;
		let filename = entry.file_name();
		let filename = filename.to_string_lossy().to_string();
		if entry.metadata()?.is_file() {
		    if filename.ends_with(".data") {
			let cask = BitcaskDatafile::new(filename)?;
		    } else if filename.ends_with(".hints") {
			let hints = bitcask_hintsfile_import(&cask.keymap, filename)?;
		    }
		}
	    }
	    Ok(cask)
	}
	pub fn get(self: &Self, key: i32) -> Option<String> {
	    None
	}
	pub fn put(self: &Self, key: i32, value: &String) -> Option<String> {
	    None
	}
	pub fn delete(self: &Self, key: i32) -> Option<String> {
	    None
	}
	//pub fn list_keys(self: Self) -> [i32] {
	//    [12]
	//}
	pub fn sync(self: &Self) -> Option<String> {
	    None
	}
	pub fn close(self: &Self) -> Option<String> {
	    None
	}
    }
}
