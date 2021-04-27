x add rabin, PutStream and PutFile test cases
x add PutFile code 
    x start with World.AppendBlock()
x implement multilevel storage
    x configurable defaults to 2 levels, 3 digits
    x isolate internal db level config from UI-visible and node
      content path strings
x implement `pb exec` and a language prototype 
  x spike, likely to inform all of the following
  x after this works, move it to its own package
x reconcile Node and blob
  x call them both Object
    x Object interface
  x merge in Key as well
x rename Node to Tree
x add file headers
x fix preimage risk 
    x right now we're doing this: Hnode = H(             "blob" || H(blob1) ||   "blob" || H(blob2) )
    x guidelines say do this:     Hnode = H(   1    || H(  0    ||   blob1) || H(  0    ||   blob2) )
    x so we should be doing this: Hnode = H( "node" || H("blob" ||   blob1) || H("blob" ||   blob2) )
    x see https://crypto.stackexchange.com/a/43434/34230 for example
      of guidelines for preventing second preimage attack
        x "The hash of the list (e1,e2) is then H(1 || h0 || h1) for h0=H(0 || e0) and h1=H(0 || e1). "
        x 1 == "node" and 0 == "blob"
    x so we need to add a header in each file identifying the object class
      x hash after adding the header
x possible alternative to merkle:
    x bloom tree: https://arxiv.org/pdf/2002.03057.pdf
x our goal isn't to keep two pieces of data from hashing to the same
  value, as in passwords -- we in fact want identical data to have the
  same hash, for deduplication.  the reason we salt is to not enable
  hash reversing of out-of-band protocols
    x from discord: "Passwords get salted to prevent the same password
      from two different users resulting in the same hash.  That's not
      exactly what I mean we should do with data blocks -- we want the
      same data to always result in the same hash.  What I mean
      instead is that, if Mallory finds a hash of the cleartext of
      some private data somewhere, and that data wasn't salted before
      hashing, we should not make it easy for Mallory to use our tool
      to fetch that private data. Making Mallory's attack easy would
      break a lot of the modern world by making one-way hashes
      reversible -- I've been worried about that vulnerability from
      the beginning.  We can resist Mallory's attack by first salting
      the private data before hashing it.  I think that we can prevent
      Mallory's attack by simply prepending the word "blob" on every
      blob before hashing it, for instance.  I think."
x start RFC 3 with the above
x split into multiple files or packages
    x close out `streaming`, make a new `split` branch
    x db, tree, stream, blob, and util
    x tests also
x clean up test directories
x clean up streaming enough to support `pb run`
    x write test case for tree.Read()
    x convert tree.Cat() and stream.Cat()
    x look around for anywhere else a buf is being returned
    x test pb with ulimit 
x write pb run:
    x see https://docs.docker.com/engine/api/sdk/examples/
    x we likely want to use save/load instead of export/import because
      of https://medium.com/@cminion/quicknote-docker-load-vs-docker-import-ed1367b93721
    x https://pspdfkit.com/blog/2019/docker-import-export-vs-load-save/
    x https://maori.geek.nz/how-to-digest-a-docker-image-ca9fc7630b71
    x https://pkg.go.dev/github.com/docker/docker/client#Client.ImageLoad

```
    host1 $ pb putstream sha256 ubuntu < /tmp/ubuntu-docker-save.tar 
    stream/ubuntu -> tree/sha256/0ebd5d411223e3777db972163a60aa2f45c386db5c2353978e95fabdd1b08b08
    host2 $ pb run --rm -it sha256/0ebd5d411223e3777db972163a60aa2f45c386db5c2353978e95fabdd1b08b08 echo hello
    hello
```

x track down source of the multiple closes on file handles
    x figure out why we can't uncomment tree.go:178
- write pit library
    - write test cases
        - protocol parser and/or inotify lib first
    - import pit library into pb
    - `pb` is the cmdline utility providing an API for shell scripts
    - move runContainer and most other pb functions into library
        - run the container in a goroutine with stdio via channels
- spike network layer
	- add daemon() to pb 
		- run daemon with `pb daemon`, 
	- move or copy pb runContainer into daemon
		- run the container in a goroutine with stdio via channels
    - client/broker/member talks to daemon via unix domain socket, stream mode
- containerize tests
    - this will also help provide a linux VM for Matt
- RFC -- UDS protocol
- write some test cases where we change the working directory
    - should help make macOS work
- move rfcs to:
    - 0000 t7a
    - 1000 pb/gdo
    - 2000 cdint
- RFC -- auth&auth
    - e.g. authorization by key fingerprint should be via encryption, not by us
- RFC -- architecture
    - db
    - daemon
    - counterparty
    - network
- RFC -- accounting records
    - one blob per transaction leg 
        - per-leg payload in stream
- make wide trees streamable
    - see note in memtest.sh
- spike pit
    - accounting
    - disk is network
    - unix authentication
        - this only makes sense if we also own /etc/passwd and /etc/group
        - but access control is only needed in cases of data
          purchase/xfer
    - move stream to trader?
    - XXX
- non-trivial dsl spike candidates:
    - `harness`
        - calls docker 
        - spike objfault handler
    - VCS
        - needs stable db 
    - RFC
        - needs stable db 
    - upper layer DSL (accounting, network, or market)
        - needs distro db -- could bootstrap via go:embed
        - we implement syscall-like primitives in Go, upper layers in DSL
            - fork -- run container with config and args
            - pull -- fetch object from remote
            - push? probably ad or quote
            - confirm?
            - commit -- add transaction to ledger
- make a tool to make hash updates easier
    - `pb mv`
    - `pb mkpath` 
    - `pb mksubst old new` - generate perl regex 
- merge ./blob and ./tree directories?  
- move stream to a higher layer, move ./stream to caller's disk space?
    - otherwise pitbase needs to auth&auth
- further research:
    - rainbow tables https://en.wikipedia.org/wiki/Rainbow_table#Precomputed_hash_chains
- unexport things that don't need exporting
- clean up golint and errcheck
- continue improving coverage
- revisit filepath.Join() vs anywhere we really want forward-slashes
- start writing test cases for possible next layer to prove or disprove the following
    - likely application is image and container management, host management, file version control, accounting, logging...
        - generically, don't forget the decentralized virtual machine model
    - write db.GetLabels(), or start on the next layer up and put it there?
    - revisit whether we want any accounting at this layer, or just provide hooks
        - start working out container communication api
- add documentation:
    - README.md
    x ROADMAP.md
    - CONTRIBUTING.md
- review exported comments
- release on github
- stress and benchmark testing
- start using github flow
- host management app
- docker image management app
- container mgmt app
- self-hosting
