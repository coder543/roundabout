# Roundabout Postgres Proxy

This is a small postgres proxy that is intended to be an alternative to PgBouncer. It avoids some of the legacy complexity of PgBouncer, like having to pick between session, transaction, or statement modes by automatically detecting transactions and anonymous prepared statements.

# Status

I would say this is a rough prototype. There are no tests, and it has not been used in a production environment. It currently lacks TLS support, and it has some references to read replicas, but does not actually support transparently redirecting queries to read replicas yet.

# Development

```bash
docker-compose up -d # this will start a postgres instance with a password matching the one in `roundabout.yml`
go run main.go # this will compile and start `roundabout`
psql -h localhost -U postgres # this will let you connect to the database through `roundabout`, and you'll need to use the password specified in `roundabout.yml` in virtual.password
```

# License

MIT

```

```
