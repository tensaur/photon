fn main() {
    let server_url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://[::1]:50052/api/query".into());
    photon_dashboard::run(server_url).unwrap();
}
