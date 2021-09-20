all: protocol.svg

protocol.svg: protocol.mscgen
	mscgen -T svg protocol.mscgen

loop:
	while true; do inotifywait protocol.mscgen; sleep 1; make; kill $$child; midori protocol.svg & child=$$!; done
