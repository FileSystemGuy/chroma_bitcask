use crate::bitcask::bitcask::Bitcask;
mod bitcask;
mod tests;

fn main() {
    let bc = Bitcask::new("foo".to_string()).expect("REASON");
    //let a = "a".to_string();
    //let b = "b".to_string();
    //bc.add(&a, &b);
    println!("All done in this universe, and all others!");
}
