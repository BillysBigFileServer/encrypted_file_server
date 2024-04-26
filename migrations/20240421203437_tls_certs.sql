CREATE TABLE tls_certs(
    id SERIAL PRIMARY KEY,
    cert_chain_pem TEXT NOT NULL,
    private_key_pem TEXT NOT NULL,
    expires_at TIMESTAMP NOT NULL
)
