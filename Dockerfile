# build stage
FROM rust:latest as builder

WORKDIR /app

COPY . .

RUN cargo build --release

# prod stage

FROM gcr.io/distroless/cc

COPY --from=builder /app/target/release/backend /

EXPOSE 8000

CMD ["./backend"]
