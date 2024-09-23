module github.com/fsnotify/fsnotify

go 1.17

require (
	github.com/bits-and-blooms/bloom/v3 v3.7.0
	github.com/panjf2000/ants/v2 v2.10.0
	golang.org/x/sys v0.13.0
)

require (
	github.com/bits-and-blooms/bitset v1.10.0 // indirect
	golang.org/x/sync v0.3.0 // indirect
)

retract (
	v1.5.3 // Published an incorrect branch accidentally https://github.com/fsnotify/fsnotify/issues/445
	v1.5.0 // Contains symlink regression https://github.com/fsnotify/fsnotify/pull/394
)
