x add rabin, PutStream and PutFile test cases
x add PutFile code 
    x start with World.AppendBlock()
- reconcile Node and blob
  - maybe call them both Object
    - maybe Object interface
  - maybe merge in Key as well
- split into multiple files or packages
    - db, node, world, and util
    - leave function_test in one file for now
- implement multilevel storage
    x configurable defaults to 2 levels, 3 digits
    - we need to isolate internal db level config from UI-visible
      and node content path strings
- continue improving coverage
- stress and benchmark testing
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
- start using github flow
- host management app
- docker image management app
- container mgmt app
- self-hosting
