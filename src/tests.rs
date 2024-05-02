//
// Tests for the Bitcask module
//
#[cfg(test)]
pub mod tests {
    use crate::Bitcask;
    use crate::test_fixture;
    use std::fs::File;

    #[test]
    fn test_get() {
	let mut file = File::create("foo.txt");
	let bc = Bitcask::new("foo".to_string()).expect("REASON");
	let val = bc.get(12);
	assert_eq!(val, None);
    }

    #[test]
    fn test_add_get() {
	let value = "b".to_string();
	let bc = Bitcask::new("foo".to_string()).expect("REASON");
	bc.put(14, &value);
	let val = bc.get(14);
	assert_eq!(val, Some(value));
    }

    #[test]
    fn test_add_delete_get() {
	let value = "b".to_string();
	let bc = Bitcask::new("foo".to_string()).expect("REASON");
	bc.put(10, &value);
	let val = bc.get(10);
	assert_eq!(val, Some(value));
	bc.delete(10);
	let val = bc.get(10);
	assert_eq!(val, None);
    }
}
