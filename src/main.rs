mod auth;
mod browser;
mod cli;
mod config;
mod explore;
mod runtime;
mod store;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if let Err(err) = cli::run(&args) {
        eprintln!("error: {err}");
        std::process::exit(err.code);
    }
}
