#!/usr/bin/env sh
set -eu

gateway_url=${1:-http://localhost:8080}
api_key=${API_KEY:-${LAB_API_KEY:-}}
house_id=${DATA_HOUSE_ID:-house-1}
gateway_url=${gateway_url%/}
: "${api_key:?Set API_KEY or LAB_API_KEY before running the seed script}"

seed_book() {
  book_id=$1
  book_json=$2
  curl --fail-with-body --silent --show-error \
    -X PUT "$gateway_url/books/$book_id" \
    -H 'Content-Type: application/json' \
    -H 'Accept: application/json' \
    -H "X-Data-House: $house_id" \
    -H "X-API-Key: $api_key" \
    -d "$book_json"
  printf '\n'
}

seed_book 000000000000000000000001 '{"title":"Dune","author":"Frank Herbert","isbn":"9780441172719"}'
seed_book 000000000000000000000002 '{"title":"1984","author":"George Orwell","isbn":"9780451524935"}'
seed_book 000000000000000000000003 '{"title":"Foundation","author":"Isaac Asimov","isbn":"9780553293357"}'
seed_book 000000000000000000000004 '{"title":"The Hobbit","author":"J. R. R. Tolkien","isbn":"9780547928227"}'
seed_book 000000000000000000000005 '{"title":"The Left Hand of Darkness","author":"Ursula K. Le Guin","isbn":"9780441478125"}'

printf 'Seeded 5 books through %s. The sync worker will copy them to the other houses.\n' "$house_id"
