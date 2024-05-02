//
// Tests for the Bitcask module
//
#[cfg(test)]
pub mod tests {
    use crate::Bitcask;

    #[test]
    fn test_get() {
	let testdir = "test_get/".to_string();
	test_setup(&testdir);
	let bc = Bitcask::new(&testdir).expect("REASON");
	let val = bc.get(12);
	assert_eq!(val, None);
	test_teardown(&testdir);
    }

    #[test]
    fn test_add_get() {
	let testdir = "test_add_get/".to_string();
	test_setup(&testdir);
	let value = "b".to_string();
	let bc = Bitcask::new(&testdir).expect("REASON");
	bc.put(14, &value);
	let val = bc.get(14);
	assert_eq!(val, Some(value));
	test_teardown(&testdir);
    }

    #[test]
    fn test_add_delete_get() {
	let testdir = "test_add_delete_get/".to_string();
	test_setup(&testdir);
	let value = "b".to_string();
	let bc = Bitcask::new(&testdir).expect("REASON");
	bc.put(10, &value);
	let val = bc.get(10);
	assert_eq!(val, Some(value));
	bc.delete(10);
	let val = bc.get(10);
	assert_eq!(val, None);
	test_teardown(&testdir);
    }

    //
    // Setup and teardown of tests, takes a unique directory name to
    // isolate ech test from the others since they all run in parallel.
    //
    fn test_setup(dirname: &String) {
	let realdirname = "TeStDiR/".to_string() + dirname;
	let _ = std::fs::create_dir_all(realdirname);
    }
    fn test_teardown(dirname: &String) {
	let realdirname = "TeStDiR/".to_string() + dirname;
	let _ = std::fs::remove_dir_all(realdirname);
    }
}
