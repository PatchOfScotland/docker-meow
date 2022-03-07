# docker-meow
mig_meow experiment container

# To run

docker build --tag dockermeow .
docker run -it -d --mount type=bind,source="$(pwd)"/scripts,target=/scripts/ --mount type=bind,source="$(pwd)"/results,target=/results/ dockermeow
docker exec -it XXXX bash
