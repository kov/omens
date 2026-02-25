mod cli;
mod config;
mod runtime;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Err(err) = cli::run(&args) {
        eprintln!("error: {err}");
        std::process::exit(40);
    }
}
