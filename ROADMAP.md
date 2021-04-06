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
- rename Node to Tree
- fix preimage risk -- leaf vs node marker M is used e.g. H( M || h1 || h2) or H(M || h1)
    - add a header in each file identifying the object class
      - hash after
- probably won't do these due to risk of preimage, and need for implicit fetch of tree entries
    - merge ./blob and ./node directories?  
    - add class column to tree lines?  
- split into multiple files or packages
    - db, node, world, and util
    - World may be a good candidate for a separate package
- continue improving coverage
- revisit filepath.Join() vs anywhere we really want forward-slashes
- start writing test cases for possible next layer to prove or disprove the following
    - likely application is image and container management, host management, file version control, accounting, logging...
        - generically, don't forget the decentralized virtual machine model
    - write world.GetLabels(), or start on the next layer up and put it there?
    - can we merge world name and label so we can do nested worlds?
    - can we apply more than one label to the same node entry?
    - revisit whether we want any accounting at this layer, or just provide hooks
        - start working out container communication api
- unexport things that don't need exporting
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
