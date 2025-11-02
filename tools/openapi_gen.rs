#[cfg(feature = "openapi")]
fn main() {
    let doc = nanocloud::nanocloud::server::handlers::build_openapi_doc();
    match doc.to_pretty_json() {
        Ok(json) => println!("{}", json),
        Err(err) => {
            eprintln!("failed to serialize OpenAPI document: {err}");
            std::process::exit(1);
        }
    }
}

#[cfg(not(feature = "openapi"))]
fn main() {
    eprintln!("the `openapi` feature must be enabled to generate the OpenAPI specification");
    std::process::exit(1);
}
