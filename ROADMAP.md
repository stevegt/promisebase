x add rabin, PutStream and PutFile test cases
x add PutFile code 
    x start with World.AppendBlock()
x implement multilevel storage
    x configurable defaults to 2 levels, 3 digits
    x isolate internal db level config from UI-visible and node
      content path strings
- reconcile Node and blob
  - maybe call them both Object
    - maybe Object interface
  - maybe merge in Key as well
- split into multiple files or packages
    - db, node, world, and util
    - leave function_test in one file for now
- continue improving coverage
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
