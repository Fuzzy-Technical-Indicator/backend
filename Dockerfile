# build stage
FROM rust:1.76-bookworm as builder

WORKDIR /app

COPY . .

RUN cargo build --release

# prod stage
FROM gcr.io/distroless/base-debian12

# copy necessary lib
ARG ARCH=x86_64
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libfontconfig.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libfreetype.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libexpat.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libz.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libpng16.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libbrotlidec.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libbrotlicommon.so* /usr/lib/${ARCH}-linux-gnu/
COPY --from=builder /usr/lib/${ARCH}-linux-gnu/libgcc_s.so* /usr/lib/${ARCH}-linux-gnu/

# copy binary 
COPY --from=builder /app/target/release/backend /app/
COPY --from=builder /app/.env /app/

WORKDIR /app

EXPOSE 8000

ENV RAYON_NUM_THREADS=7

CMD ["./backend"]
