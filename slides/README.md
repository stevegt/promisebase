class: center, middle

# PromiseBase: A Content-Addressable Storage System

---

# What is PromiseBase?

PromiseBase (also called "pitbase" in the code) is a **content-addressable storage (CAS)** system that stores data by its cryptographic hash rather than by filename or location.

Key features include:
- **Deduplication**: Identical data stored only once
- **Merkle trees**: Hierarchical organization with cryptographic verification
- **Streaming support**: Handles large files via content-defined chunking
- **FUSE filesystem**: Mount the database as a regular filesystem
- **Container integration**: Store and run Docker containers from the database

The system addresses the fundamental problem of data duplication and provides a foundation for distributed systems, version control, and container management.

---

# Why is this Important?

PromiseBase tackles several critical problems in modern computing:

**Data Deduplication**: Eliminates redundant storage of identical content, saving disk space and bandwidth. In a world where the same container images, datasets, and files are copied endlessly across systems, this provides massive efficiency gains.

**Content Verification**: Using cryptographic hashes as addresses makes data corruption immediately detectable. You can't retrieve corrupted data without knowing it's corrupted.

**Immutable Storage**: Once stored, content cannot be changed without changing its address. This provides strong guarantees for reproducibility and auditing.

**Distributed Systems Foundation**: Content-addressable storage is the building block for peer-to-peer networks, blockchain systems, and distributed version control systems like Git.

**Container Storage**: Modern container ecosystems involve massive duplication of layers and images. A CAS approach can dramatically reduce storage requirements.

---

# Who Wrote Promisebase?

PromiseBase was written by:

- **Matt Nordling** (m@nordling.org) 
- **Ryan Hair** (xfactor529@gmail.com) 
- **Steve Traugott** (stevegt@t7a.org) 
- **Angela Traugott** (angela@t7a.org)
- **Jessica Traugott** (jessica@t7a.org)

Previous related work by:

- Colin Bradley
- Courtney Chu

---

# When Was Promisebase Written?

**Timeline: 2020-2021**


**Global Context:**
- **COVID-19 pandemic**: Massive shift to remote work and distributed computing
- **Container explosion**: Docker and Kubernetes becoming ubiquitous
- **Edge computing**: Need for efficient data distribution to edge nodes
- **Supply chain**: Software and hardware supply chain security concerns
- **Decentralization**: Growing interest in decentralized systems and peer-to-peer networks

This timing makes sense - remote work highlighted inefficiencies in data distribution, computation, and storage.

---

# How Was Promisebase Written?

**Development Methodology:**

- Mob programming and pair programming using:
    - [remote mob programming](https://www.remotemobprogramming.org/)
      rules
    - [mob.sh](http://mob.sh)
    - [mob-consensus](https://gist.github.com/stevegt/2c04ee0e9500ff1727eff60e538934a1)

**Agile Development Practices:**
- Iterative development with frequent testing cycles
- Test-driven development with comprehensive coverage requirements

**Quality Assurance:**
- Automated testing with `covertest.sh` enforcing minimum coverage
- Performance benchmarking and memory testing
- Error checking with `errcheck` and linting with `golint`
- Comprehensive integration tests using Google's cmdtest framework

---

# What Tools Were Used?

**Key Dependencies:**
- **go-fuse v2**: FUSE filesystem interface for Linux/macOS
- **restic/chunker**: Rabin fingerprinting for content-defined chunking
- **Docker client library**: Container integration and image management
- **msgpack**: Binary serialization for efficient data storage

**Development Tools:**
- **Standard Go tooling**: go test, go build, coverage analysis
- **Docker/Skopeo**: Container image manipulation

**Testing & Quality:**
- Comprehensive test suite with coverage reporting (`covertest.sh`)
- Benchmark tests for performance validation (`memtest.sh`)
- Error checking with custom goadapt library
- Integration testing with cmdtest framework

---

# What Does the Code Do? (1/2)

**Core Database Operations:**
- **PutBlock**: Store binary data, return SHA-256/SHA-512 address
- **GetBlock**: Retrieve data by cryptographic address
- **PutTree**: Create Merkle tree nodes pointing to blocks or other trees
- **GetTree**: Retrieve and verify tree structures

**Streaming & Chunking:**
- **PutStream**: Break large files into chunks using Rabin fingerprinting
- **Chunking Algorithm**: Content-defined boundaries for optimal deduplication
- **Tree Assembly**: Automatically build Merkle trees from chunked streams

**Container Integration:**
- Store Docker images as content-addressable trees
- Run containers directly from database storage
- Image deduplication across container layers

---

# What Does the Code Do? (2/2)

**Filesystem Interface:**
- **FUSE mounting**: Access database content as regular filesystem
- **Virtual directories**: Browse data by hash algorithms (sha256/, sha512/)
- **Content files**: Read tree content through filesystem API

**Command Line Tools:**
- **pb**: Full-featured CLI for all database operations
- **pitd**: Background daemon for serving requests
- **Streaming commands**: catstream, putstream for large data handling

**Security Features:**
- **Preimage attack prevention**: Salt data with type prefixes ("block\n", "tree\n")
- **Cryptographic verification**: All content automatically verified on read
- **Immutable storage**: Content addresses cannot be forged

---

class: diagram

# High-Level Architecture Diagram (old)

<svg width="290pt" height="268pt"
 viewBox="0.00 0.00 289.50 268.00" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
<g id="graph0" class="graph" transform="scale(1 1) rotate(0) translate(4 264)">
<title>architecture</title>
<polygon fill="white" stroke="transparent" points="-4,4 -4,-264 285.5,-264 285.5,4 -4,4"/>
<!-- CLI -->
<g id="node1" class="node">
<title>CLI</title>
<polygon fill="none" stroke="black" points="173.5,-260 108.5,-260 108.5,-222 173.5,-222 173.5,-260"/>
<text text-anchor="middle" x="141" y="-244.8" font-family="Times,serif" font-size="14.00">pb CLI</text>
<text text-anchor="middle" x="141" y="-229.8" font-family="Times,serif" font-size="14.00">Interface</text>
</g>
<!-- Blocks -->
<g id="node4" class="node">
<title>Blocks</title>
<polygon fill="none" stroke="black" points="246,-112 178,-112 178,-74 246,-74 246,-112"/>
<text text-anchor="middle" x="212" y="-96.8" font-family="Times,serif" font-size="14.00">Blocks</text>
<text text-anchor="middle" x="212" y="-81.8" font-family="Times,serif" font-size="14.00">(Content)</text>
</g>
<!-- CLI&#45;&gt;Blocks -->
<g id="edge1" class="edge">
<title>CLI&#45;&gt;Blocks</title>
<path fill="none" stroke="black" d="M160.81,-221.64C170.28,-211.88 181.04,-199.19 188,-186 198.55,-166.02 204.68,-141.1 208.1,-122.24"/>
<polygon fill="black" stroke="black" points="211.58,-122.62 209.77,-112.18 204.68,-121.47 211.58,-122.62"/>
</g>
<!-- Trees -->
<g id="node5" class="node">
<title>Trees</title>
<polygon fill="none" stroke="black" points="78.5,-112 13.5,-112 13.5,-74 78.5,-74 78.5,-112"/>
<text text-anchor="middle" x="46" y="-96.8" font-family="Times,serif" font-size="14.00">Trees</text>
<text text-anchor="middle" x="46" y="-81.8" font-family="Times,serif" font-size="14.00">(Merkle)</text>
</g>
<!-- CLI&#45;&gt;Trees -->
<g id="edge2" class="edge">
<title>CLI&#45;&gt;Trees</title>
<path fill="none" stroke="black" d="M112.01,-221.97C99.08,-212.64 84.63,-200.22 75,-186 62.01,-166.81 54.59,-141.49 50.51,-122.27"/>
<polygon fill="black" stroke="black" points="53.92,-121.44 48.58,-112.29 47.05,-122.78 53.92,-121.44"/>
</g>
<!-- Streams -->
<g id="node6" class="node">
<title>Streams</title>
<polygon fill="none" stroke="black" points="145.5,-186 84.5,-186 84.5,-148 145.5,-148 145.5,-186"/>
<text text-anchor="middle" x="115" y="-170.8" font-family="Times,serif" font-size="14.00">Streams</text>
<text text-anchor="middle" x="115" y="-155.8" font-family="Times,serif" font-size="14.00">(refs)</text>
</g>
<!-- CLI&#45;&gt;Streams -->
<g id="edge3" class="edge">
<title>CLI&#45;&gt;Streams</title>
<path fill="none" stroke="black" d="M134.44,-221.83C131.6,-213.96 128.21,-204.57 125.06,-195.85"/>
<polygon fill="black" stroke="black" points="128.34,-194.63 121.65,-186.41 121.75,-197.01 128.34,-194.63"/>
</g>
<!-- FUSE -->
<g id="node2" class="node">
<title>FUSE</title>
<polygon fill="none" stroke="black" points="90,-260 0,-260 0,-222 90,-222 90,-260"/>
<text text-anchor="middle" x="45" y="-244.8" font-family="Times,serif" font-size="14.00">FUSE Mount</text>
<text text-anchor="middle" x="45" y="-229.8" font-family="Times,serif" font-size="14.00">Interface</text>
</g>
<!-- FUSE&#45;&gt;Blocks -->
<g id="edge4" class="edge">
<title>FUSE&#45;&gt;Blocks</title>
<path fill="none" stroke="black" d="M90.01,-224.28C111.1,-215.36 135.58,-202.62 154,-186 174,-167.95 189.72,-141.45 199.72,-121.52"/>
<polygon fill="black" stroke="black" points="202.96,-122.86 204.16,-112.33 196.66,-119.81 202.96,-122.86"/>
</g>
<!-- FUSE&#45;&gt;Trees -->
<g id="edge5" class="edge">
<title>FUSE&#45;&gt;Trees</title>
<path fill="none" stroke="black" d="M43.65,-221.67C42.46,-203.1 41.05,-173.55 42,-148 42.3,-139.77 42.87,-130.85 43.48,-122.67"/>
<polygon fill="black" stroke="black" points="46.99,-122.72 44.29,-112.48 40.01,-122.17 46.99,-122.72"/>
</g>
<!-- FUSE&#45;&gt;Streams -->
<g id="edge6" class="edge">
<title>FUSE&#45;&gt;Streams</title>
<path fill="none" stroke="black" d="M62.66,-221.83C70.97,-213.28 81.03,-202.94 90.09,-193.62"/>
<polygon fill="black" stroke="black" points="92.64,-196.02 97.1,-186.41 87.62,-191.14 92.64,-196.02"/>
</g>
<!-- PITD -->
<g id="node3" class="node">
<title>PITD</title>
<polygon fill="none" stroke="black" points="281.5,-260 218.5,-260 218.5,-222 281.5,-222 281.5,-260"/>
<text text-anchor="middle" x="250" y="-244.8" font-family="Times,serif" font-size="14.00">pitd</text>
<text text-anchor="middle" x="250" y="-229.8" font-family="Times,serif" font-size="14.00">Daemon</text>
</g>
<!-- PITD&#45;&gt;Blocks -->
<g id="edge7" class="edge">
<title>PITD&#45;&gt;Blocks</title>
<path fill="none" stroke="black" d="M258.24,-221.72C265.58,-202.67 273.79,-172.27 264,-148 259.68,-137.28 252.1,-127.5 244.04,-119.29"/>
<polygon fill="black" stroke="black" points="246.24,-116.57 236.58,-112.22 241.43,-121.64 246.24,-116.57"/>
</g>
<!-- PITD&#45;&gt;Trees -->
<g id="edge8" class="edge">
<title>PITD&#45;&gt;Trees</title>
<path fill="none" stroke="black" d="M234.04,-221.71C216.02,-201.82 185.13,-169.94 154,-148 142.4,-139.83 112.87,-125.2 87.61,-113.22"/>
<polygon fill="black" stroke="black" points="89.08,-110.05 78.54,-108.95 86.1,-116.38 89.08,-110.05"/>
</g>
<!-- PITD&#45;&gt;Streams -->
<g id="edge9" class="edge">
<title>PITD&#45;&gt;Streams</title>
<path fill="none" stroke="black" d="M218.35,-223.12C199.29,-212.95 174.86,-199.93 154.54,-189.09"/>
<polygon fill="black" stroke="black" points="155.99,-185.9 145.52,-184.28 152.7,-192.07 155.99,-185.9"/>
</g>
<!-- WORM -->
<g id="node8" class="node">
<title>WORM</title>
<polygon fill="none" stroke="black" points="175,-38 83,-38 83,0 175,0 175,-38"/>
<text text-anchor="middle" x="129" y="-22.8" font-family="Times,serif" font-size="14.00">WORM Files</text>
<text text-anchor="middle" x="129" y="-7.8" font-family="Times,serif" font-size="14.00">(Content)</text>
</g>
<!-- Blocks&#45;&gt;WORM -->
<g id="edge10" class="edge">
<title>Blocks&#45;&gt;WORM</title>
<path fill="none" stroke="black" d="M191.06,-73.83C180.93,-65.05 168.62,-54.37 157.65,-44.85"/>
<polygon fill="black" stroke="black" points="159.79,-42.07 149.94,-38.16 155.2,-47.36 159.79,-42.07"/>
</g>
<!-- Trees&#45;&gt;WORM -->
<g id="edge11" class="edge">
<title>Trees&#45;&gt;WORM</title>
<path fill="none" stroke="black" d="M66.94,-73.83C77.07,-65.05 89.38,-54.37 100.35,-44.85"/>
<polygon fill="black" stroke="black" points="102.8,-47.36 108.06,-38.16 98.21,-42.07 102.8,-47.36"/>
</g>
<!-- Streams&#45;&gt;Blocks -->
<g id="edge13" class="edge">
<title>Streams&#45;&gt;Blocks</title>
<path fill="none" stroke="black" d="M139.48,-147.83C151.54,-138.88 166.25,-127.96 179.27,-118.3"/>
<polygon fill="black" stroke="black" points="181.59,-120.93 187.53,-112.16 177.41,-115.31 181.59,-120.93"/>
</g>
<!-- Streams&#45;&gt;Trees -->
<g id="edge12" class="edge">
<title>Streams&#45;&gt;Trees</title>
<path fill="none" stroke="black" d="M97.59,-147.83C89.48,-139.37 79.68,-129.15 70.82,-119.9"/>
<polygon fill="black" stroke="black" points="73.09,-117.21 63.65,-112.41 68.04,-122.05 73.09,-117.21"/>
</g>
<!-- Chunker -->
<g id="node7" class="node">
<title>Chunker</title>
<polygon fill="none" stroke="black" points="159.5,-112 96.5,-112 96.5,-74 159.5,-74 159.5,-112"/>
<text text-anchor="middle" x="128" y="-96.8" font-family="Times,serif" font-size="14.00">Chunker</text>
<text text-anchor="middle" x="128" y="-81.8" font-family="Times,serif" font-size="14.00">(Rabin)</text>
</g>
<!-- Streams&#45;&gt;Chunker -->
<g id="edge14" class="edge">
<title>Streams&#45;&gt;Chunker</title>
<path fill="none" stroke="black" d="M118.28,-147.83C119.67,-140.13 121.32,-130.97 122.87,-122.42"/>
<polygon fill="black" stroke="black" points="126.34,-122.88 124.68,-112.41 119.45,-121.63 126.34,-122.88"/>
</g>
</g>
</svg>

---

class: diagram

# High-Level Architecture Diagram (work in progress)


<svg width="600pt" height="320pt"
 viewBox="0.00 0.00 941.50 617.00" xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">
<g id="graph0" class="graph" transform="scale(1 1) rotate(0) translate(4 613)">
<title>Layers</title>
<polygon fill="white" stroke="transparent" points="-4,4 -4,-613 937.5,-613 937.5,4 -4,4"/>
<!-- UI -->
<g id="node1" class="node">
<title>UI</title>
<polygon fill="white" stroke="black" points="701.5,-609 519.5,-609 519.5,-571 701.5,-571 701.5,-609"/>
<text text-anchor="middle" x="610.5" y="-593.8" font-family="monospace" font-size="14.00">User Interface Layer</text>
<text text-anchor="middle" x="610.5" y="-578.8" font-family="monospace" font-size="14.00">(CLI, Web, FUSE)</text>
</g>
<!-- VCS -->
<g id="node3" class="node">
<title>VCS</title>
<polygon fill="white" stroke="black" points="333,-505 242,-505 242,-469 333,-469 333,-505"/>
<text text-anchor="middle" x="287.5" y="-483.3" font-family="monospace" font-size="14.00">VCS Layer</text>
</g>
<!-- UI&#45;&gt;VCS -->
<g id="edge1" class="edge">
<title>UI&#45;&gt;VCS</title>
<path fill="none" stroke="red" d="M519.48,-582.9C460.32,-577.58 389.52,-568.27 363.5,-553 349.01,-544.5 352.79,-534.45 340.5,-523 335.89,-518.71 330.71,-514.59 325.43,-510.78"/>
<polygon fill="red" stroke="red" points="327.34,-507.85 317.12,-505.07 323.38,-513.62 327.34,-507.85"/>
<text text-anchor="middle" x="447" y="-541.8" font-family="Times,serif" font-size="14.00" fill="red">Commit(), Branch(), Merge(),</text>
<text text-anchor="middle" x="447" y="-526.8" font-family="Times,serif" font-size="14.00" fill="red">Log(), Checkout()</text>
</g>
<!-- DirTree -->
<g id="node4" class="node">
<title>DirTree</title>
<polygon fill="white" stroke="black" points="632.5,-403 450.5,-403 450.5,-367 632.5,-367 632.5,-403"/>
<text text-anchor="middle" x="541.5" y="-381.3" font-family="monospace" font-size="14.00">Directory Tree Layer</text>
</g>
<!-- UI&#45;&gt;DirTree -->
<g id="edge2" class="edge">
<title>UI&#45;&gt;DirTree</title>
<path fill="none" stroke="blue" d="M596.13,-570.81C584.06,-554.52 567.39,-529.51 558.5,-505 547.52,-474.75 543.6,-437.77 542.22,-413.14"/>
<polygon fill="blue" stroke="blue" points="545.71,-412.91 541.76,-403.08 538.72,-413.23 545.71,-412.91"/>
<text text-anchor="middle" x="610.5" y="-490.8" font-family="Times,serif" font-size="14.00" fill="blue">Import(), Export(),</text>
<text text-anchor="middle" x="610.5" y="-475.8" font-family="Times,serif" font-size="14.00" fill="blue">Diff(), Cat()</text>
</g>
<!-- Stream -->
<g id="node5" class="node">
<title>Stream</title>
<polygon fill="white" stroke="black" points="615.5,-300 475.5,-300 475.5,-264 615.5,-264 615.5,-300"/>
<text text-anchor="middle" x="545.5" y="-278.3" font-family="monospace" font-size="14.00">Streaming Layer</text>
</g>
<!-- UI&#45;&gt;Stream -->
<g id="edge3" class="edge">
<title>UI&#45;&gt;Stream</title>
<path fill="none" stroke="green" d="M665.16,-570.9C733.1,-545 836.46,-492.48 804.5,-421 776.75,-358.93 750.59,-350.79 690.5,-319 670.52,-308.43 647.34,-300.88 625.45,-295.53"/>
<polygon fill="green" stroke="green" points="626.03,-292.07 615.5,-293.22 624.45,-298.89 626.03,-292.07"/>
<text text-anchor="middle" x="871.5" y="-439.8" font-family="Times,serif" font-size="14.00" fill="green">NewStream(), Write(),</text>
<text text-anchor="middle" x="871.5" y="-424.8" font-family="Times,serif" font-size="14.00" fill="green">Read(), Close()</text>
</g>
<!-- Refs -->
<g id="node6" class="node">
<title>Refs</title>
<polygon fill="white" stroke="black" points="439.5,-301 249.5,-301 249.5,-263 439.5,-263 439.5,-301"/>
<text text-anchor="middle" x="344.5" y="-285.8" font-family="monospace" font-size="14.00">Reference Index Layer</text>
<text text-anchor="middle" x="344.5" y="-270.8" font-family="monospace" font-size="14.00">(Index into Messages)</text>
</g>
<!-- UI&#45;&gt;Refs -->
<g id="edge4" class="edge">
<title>UI&#45;&gt;Refs</title>
<path fill="none" stroke="purple" d="M625.99,-570.77C638.6,-554.66 655.41,-529.91 662.5,-505 668.15,-485.12 676.43,-334.27 662.5,-319 647.76,-302.84 488.21,-303.6 466.5,-301 461.05,-300.35 455.47,-299.65 449.84,-298.91"/>
<polygon fill="purple" stroke="purple" points="450.05,-295.41 439.67,-297.56 449.12,-302.35 450.05,-295.41"/>
<text text-anchor="middle" x="732" y="-439.8" font-family="Times,serif" font-size="14.00" fill="purple">SetRef(), GetRef(),</text>
<text text-anchor="middle" x="732" y="-424.8" font-family="Times,serif" font-size="14.00" fill="purple">DeleteRef(), ListRefs()</text>
</g>
<!-- MsgTimeline -->
<g id="node2" class="node">
<title>MsgTimeline</title>
<polygon fill="white" stroke="black" points="437,-211 222,-211 222,-175 437,-175 437,-211"/>
<text text-anchor="middle" x="329.5" y="-189.3" font-family="monospace" font-size="14.00">Message &amp; Timeline Layer</text>
</g>
<!-- HashKV -->
<g id="node7" class="node">
<title>HashKV</title>
<polygon fill="white" stroke="black" points="478,-123 181,-123 181,-87 478,-87 478,-123"/>
<text text-anchor="middle" x="329.5" y="-101.3" font-family="monospace" font-size="14.00">Content&#45;Addressable (hashkv) Layer</text>
</g>
<!-- MsgTimeline&#45;&gt;HashKV -->
<g id="edge12" class="edge">
<title>MsgTimeline&#45;&gt;HashKV</title>
<path fill="none" stroke="darkorange" d="M329.5,-174.6C329.5,-162.75 329.5,-146.82 329.5,-133.29"/>
<polygon fill="darkorange" stroke="darkorange" points="333,-133.08 329.5,-123.08 326,-133.08 333,-133.08"/>
<text text-anchor="middle" x="387" y="-144.8" font-family="Times,serif" font-size="14.00" fill="darkorange">Put(), Get(), Delete()</text>
</g>
<!-- VCS&#45;&gt;MsgTimeline -->
<g id="edge5" class="edge">
<title>VCS&#45;&gt;MsgTimeline</title>
<path fill="none" stroke="orange" d="M259.5,-468.95C215.82,-440.02 139.33,-378.64 168.5,-319 191.89,-271.18 243.39,-236.39 281.88,-215.84"/>
<polygon fill="orange" stroke="orange" points="283.61,-218.89 290.87,-211.17 280.38,-212.68 283.61,-218.89"/>
<text text-anchor="middle" x="245" y="-337.8" font-family="Times,serif" font-size="14.00" fill="orange">RecordMessage(),</text>
<text text-anchor="middle" x="245" y="-322.8" font-family="Times,serif" font-size="14.00" fill="orange">GetMessage(), GetParents()</text>
</g>
<!-- VCS&#45;&gt;DirTree -->
<g id="edge6" class="edge">
<title>VCS&#45;&gt;DirTree</title>
<path fill="none" stroke="brown" d="M315.68,-468.83C324.89,-463.18 335.15,-456.85 344.5,-451 365.5,-437.85 368.66,-430.62 391.5,-421 406.95,-414.49 423.83,-409.07 440.51,-404.6"/>
<polygon fill="brown" stroke="brown" points="441.49,-407.96 450.3,-402.07 439.74,-401.18 441.49,-407.96"/>
<text text-anchor="middle" x="461" y="-432.3" font-family="Times,serif" font-size="14.00" fill="brown">Import(), Export(), Diff()</text>
</g>
<!-- VCS&#45;&gt;Refs -->
<g id="edge7" class="edge">
<title>VCS&#45;&gt;Refs</title>
<path fill="none" stroke="hotpink" d="M292.36,-468.69C302.23,-433.53 324.72,-353.46 336.68,-310.86"/>
<polygon fill="hotpink" stroke="hotpink" points="340.07,-311.71 339.41,-301.13 333.33,-309.82 340.07,-311.71"/>
<text text-anchor="middle" x="371.5" y="-381.3" font-family="Times,serif" font-size="14.00" fill="hotpink">SetRef(), GetRef()</text>
</g>
<!-- DirTree&#45;&gt;Stream -->
<g id="edge8" class="edge">
<title>DirTree&#45;&gt;Stream</title>
<path fill="none" stroke="cyan" d="M542.18,-366.87C542.79,-351.41 543.7,-328.42 544.41,-310.41"/>
<polygon fill="cyan" stroke="cyan" points="547.92,-310.35 544.82,-300.22 540.93,-310.07 547.92,-310.35"/>
<text text-anchor="middle" x="605.5" y="-337.8" font-family="Times,serif" font-size="14.00" fill="cyan">NewStream(), Write(),</text>
<text text-anchor="middle" x="605.5" y="-322.8" font-family="Times,serif" font-size="14.00" fill="cyan">Read(), Close()</text>
</g>
<!-- DirTree&#45;&gt;Refs -->
<g id="edge9" class="edge">
<title>DirTree&#45;&gt;Refs</title>
<path fill="none" stroke="magenta" d="M508.08,-366.87C475.33,-350.08 425.21,-324.38 389.07,-305.85"/>
<polygon fill="magenta" stroke="magenta" points="390.24,-302.52 379.75,-301.07 387.05,-308.75 390.24,-302.52"/>
<text text-anchor="middle" x="496" y="-330.3" font-family="Times,serif" font-size="14.00" fill="magenta">GetRef()</text>
</g>
<!-- Stream&#45;&gt;MsgTimeline -->
<g id="edge10" class="edge">
<title>Stream&#45;&gt;MsgTimeline</title>
<path fill="none" stroke="darkgreen" d="M496.16,-264C479.94,-258.2 461.87,-251.53 445.5,-245 422.38,-235.77 397.03,-224.73 375.94,-215.29"/>
<polygon fill="darkgreen" stroke="darkgreen" points="377.13,-211.99 366.58,-211.08 374.26,-218.37 377.13,-211.99"/>
<text text-anchor="middle" x="494" y="-233.8" font-family="Times,serif" font-size="14.00" fill="darkgreen">RecordMessage()</text>
</g>
<!-- Rabin -->
<g id="node9" class="node">
<title>Rabin</title>
<polygon fill="white" stroke="black" points="640.5,-212 458.5,-212 458.5,-174 640.5,-174 640.5,-212"/>
<text text-anchor="middle" x="549.5" y="-196.8" font-family="monospace" font-size="14.00">Rabin Chunking</text>
<text text-anchor="middle" x="549.5" y="-181.8" font-family="monospace" font-size="14.00">(in&#45;memory chunking)</text>
</g>
<!-- Stream&#45;&gt;Rabin -->
<g id="edge14" class="edge">
<title>Stream&#45;&gt;Rabin</title>
<path fill="none" stroke="crimson" d="M546.29,-263.81C546.83,-252.01 547.57,-236.07 548.19,-222.4"/>
<polygon fill="crimson" stroke="crimson" points="551.71,-222.2 548.67,-212.05 544.71,-221.88 551.71,-222.2"/>
<text text-anchor="middle" x="569" y="-233.8" font-family="Times,serif" font-size="14.00" fill="crimson">chunk()</text>
</g>
<!-- Refs&#45;&gt;MsgTimeline -->
<g id="edge11" class="edge">
<title>Refs&#45;&gt;MsgTimeline</title>
<path fill="none" stroke="darkblue" d="M335.95,-262.69C333.75,-257.13 331.67,-250.92 330.5,-245 328.99,-237.35 328.4,-228.94 328.27,-221.19"/>
<polygon fill="darkblue" stroke="darkblue" points="331.77,-221.19 328.35,-211.16 324.77,-221.13 331.77,-221.19"/>
<text text-anchor="middle" x="369" y="-233.8" font-family="Times,serif" font-size="14.00" fill="darkblue">GetMessage()</text>
</g>
<!-- KV -->
<g id="node8" class="node">
<title>KV</title>
<polygon fill="white" stroke="black" points="371,-36 288,-36 288,0 371,0 371,-36"/>
<text text-anchor="middle" x="329.5" y="-14.3" font-family="monospace" font-size="14.00">KV Layer</text>
</g>
<!-- HashKV&#45;&gt;KV -->
<g id="edge13" class="edge">
<title>HashKV&#45;&gt;KV</title>
<path fill="none" stroke="darkviolet" d="M329.5,-86.8C329.5,-75.16 329.5,-59.55 329.5,-46.24"/>
<polygon fill="darkviolet" stroke="darkviolet" points="333,-46.18 329.5,-36.18 326,-46.18 333,-46.18"/>
<text text-anchor="middle" x="387" y="-57.8" font-family="Times,serif" font-size="14.00" fill="darkviolet">Get(), Put(), Delete()</text>
</g>
<!-- ContainerManager -->
<g id="node10" class="node">
<title>ContainerManager</title>
<polygon fill="white" stroke="black" points="149,-608 0,-608 0,-572 149,-572 149,-608"/>
<text text-anchor="middle" x="74.5" y="-586.3" font-family="monospace" font-size="14.00">ContainerManager</text>
</g>
<!-- ContainerManager&#45;&gt;VCS -->
<g id="edge15" class="edge">
<title>ContainerManager&#45;&gt;VCS</title>
<path fill="none" stroke="navy" d="M80.86,-571.86C87.42,-556.62 99.16,-534.94 116.5,-523 150.23,-499.77 196.01,-491.32 231.71,-488.51"/>
<polygon fill="navy" stroke="navy" points="232.22,-491.98 241.97,-487.83 231.76,-485 232.22,-491.98"/>
<text text-anchor="middle" x="174" y="-541.8" font-family="Times,serif" font-size="14.00" fill="navy">Commit(), Branch(),</text>
<text text-anchor="middle" x="174" y="-526.8" font-family="Times,serif" font-size="14.00" fill="navy">Checkout()</text>
</g>
<!-- BareMetalManager -->
<g id="node11" class="node">
<title>BareMetalManager</title>
<polygon fill="white" stroke="black" points="316,-608 167,-608 167,-572 316,-572 316,-608"/>
<text text-anchor="middle" x="241.5" y="-586.3" font-family="monospace" font-size="14.00">BareMetalManager</text>
</g>
<!-- BareMetalManager&#45;&gt;VCS -->
<g id="edge16" class="edge">
<title>BareMetalManager&#45;&gt;VCS</title>
<path fill="none" stroke="teal" d="M237.84,-571.78C235.78,-557.87 234.93,-538.18 242.5,-523 244.44,-519.11 247.03,-515.53 250,-512.26"/>
<polygon fill="teal" stroke="teal" points="252.61,-514.62 257.48,-505.21 247.81,-509.52 252.61,-514.62"/>
<text text-anchor="middle" x="291.5" y="-541.8" font-family="Times,serif" font-size="14.00" fill="teal">Commit(), Diff(),</text>
<text text-anchor="middle" x="291.5" y="-526.8" font-family="Times,serif" font-size="14.00" fill="teal">Checkout()</text>
</g>
</g>
</svg>

---

# How is the Code Organized?

```
promisebase/
├── db/              # Core database engine
│   ├── db.go        # Main database interface
│   ├── file.go      # WORM (Write Once Read Many) file handling
│   ├── tree.go      # Merkle tree implementation
│   ├── chunker.go   # Content-defined chunking
│   └── stream.go    # Large file streaming support
│
├── fuse/            # FUSE filesystem interface
│   └── fuse.go      # Virtual filesystem implementation
│
├── cmd/
│   ├── pb/          # Command-line client
│   └── pitd/        # Background daemon
│
├── server/          # Network daemon functionality
├── client/          # Client library (stub)
└── rfc/            # Design documents and specifications
```

**Design Pattern**: Clean separation between storage engine, interfaces, and applications.

---

# Key Algorithms and Data Structures

**Content-Defined Chunking (Rabin Fingerprinting):**
- Uses rolling hash to find natural chunk boundaries
- Produces consistent chunks regardless of insertion/deletion
- Optimal for deduplication of similar files
- Configurable chunk size limits (512KB - 8MB default)

**Merkle Trees:**
- Each tree node contains hashes of child nodes
- Enables efficient verification of large datasets
- Supports both leaf nodes (blocks) and internal nodes (trees)
- File format: plain text list of child addresses

---

# Disk Storage (old)

**WORM Storage (Write Once Read Many):**
- Immutable files with cryptographic verification
- Automatic hash computation during write
- Content salting prevents preimage attacks

**Path Resolution:**
- Multi-level directory structure (depth configurable)
- Hash-based addressing with collision resistance
- Canonical vs. absolute path resolution

**Path Structure:**
```
block/sha256/d17/370/d173706e5ab6e45e3f99389002d085dc6ad663d4b8140cd98387708196425ab2
└─┬─┘ └──┬─┘ └┬┘ └┬┘ └────────────────────┬────────────────────┘
class  algo  subdir   full hash for easy debugging
```

---

# Disk Storage (new)

- use IPFS Content Identifiers (CIDs) for addressing

**Example CID:**

```
zb2rhe5P4gXftAwvA4eXQ5HJwsER2owDyS9sKaQRRVQPn93bA
```

**Human readable translation:**

```
base58btc - cidv1 - raw - sha2-256-256-6e6ff7950a36187a801613426e858dce686cd7d7e3c0fc42ee0330072d245c95
```

---

# Path Structure (new)

In order to keep directory sizes manageable, CIDs are split into path
components of 2 characters each.  The split depth is dynamic, based on
statistical performance at runtime; large (slow) directories will be
split when detected.

**This CID:**
```
gm3dkmjrmeydambxme2den3dhe2tcmlghezdantcg5stcoldhe2toyrtgmzdiolemnrgczbvmqydkzbtha2wgojwgq2dmnzzgzqwmnjaeawqu
```

**Might be saved as:**
```
gm/3d/km/jr/gm3dkmjrmeydambxme2den3dhe2tcmlghezdantcg5stcoldhe2toyrtgmzdiolemnrgczbvmqydkzbtha2wgojwgq2dmnzzgzqwmnjaeawqu
```


---

# Important Files and Their Roles

**Core Database (`db/` package):**
- `db.go`: Main database interface, initialization, high-level operations
- `file.go`: Low-level file I/O, WORM semantics, hash computation  
- `tree.go`: Merkle tree operations, traversal, verification
- `chunker.go`: Content-defined chunking using Rabin fingerprinting
- `path.go`: Address parsing, directory structure management
- `pitbase.go`: Utilities, logging, hash algorithms, object interface

**User Interfaces:**
- `cmd/pb/pbmain.go`: Full-featured command-line interface (881 lines)
- `fuse/fuse.go`: FUSE filesystem implementation for mounting database
- `server/server.go`: Network daemon with container integration

**Development & Testing:**
- `Makefile`: Test automation, coverage reporting, profiling
- `covertest.sh`: Advanced test runner with coverage enforcement
- `rfc/`: Design documents explaining architecture decisions

---

# Work in Progress & Current Issues

**Multi-process Safety**: 
- Current implementation not safe for concurrent access
- Need proper file locking and coordination

**Performance Optimization:**
- Memory usage needs optimization for large trees
- `memtest.sh` shows memory limits around 50-100MB
- Wide trees not fully streamable yet

**Network Layer (In Progress):**
- Client/server architecture partially implemented
- UNIX domain socket protocol designed but not complete
- Distributed peer-to-peer functionality planned

**Container Integration:**
- Docker image handling works but needs refinement
- OCI image format support in development
- Container execution through stored images experimental

---

# What's Missing: Future Plans

- Peer-to-peer networking for content distribution  
- Consensus mechanisms for distributed operation
- Cross-node data synchronization and caching
- Accounting and payment systems for resource usage
- Digital signatures and access control
- Integration with other CAS systems (IPFS, Perkeep)
- Windows and macOS support improvements
- Better container runtime integration
- Enhanced streaming for very large datasets
- Self-hosting capabilities
- Production deployment tooling
- Enhanced monitoring and management tools

---

# Technical Innovations

**Preimage Attack Prevention:**
The system prepends type information ("block\n", "tree\n") to all stored content before hashing. This prevents attackers from using the database to reverse known hashes of private data.

**Streaming Merkle Trees:**
Unlike traditional Merkle trees, this implementation supports streaming reads of arbitrarily large trees without loading entire structures into memory.

**Container-Native Storage:**
Direct integration with Docker and OCI formats, allowing containers to be stored and executed directly from the content-addressable database.

**FUSE Integration:**
Provides a standard filesystem interface, making the database accessible to any POSIX-compliant application without modification.

**Content-Defined Chunking:**
Uses Rabin fingerprinting to create consistent chunk boundaries, maximizing deduplication even when files are modified.

---

# Architecture Philosophy

**UNIX Philosophy:**
- Small, focused components that do one thing well
- Clean interfaces between database, filesystem, and network layers
- Command-line tools for scripting and automation

**Immutable Infrastructure:**
- Once stored, content never changes
- All modifications create new addresses
- Perfect audit trail and reproducibility

**Content-Centric Design:**
- Data location determined by content, not naming
- Natural deduplication and integrity checking
- Enables efficient distribution and caching

**Layered Architecture:**
- Core storage engine independent of interfaces
- Multiple access methods (CLI, FUSE, network)
- Extensible design for future protocols

---

class: center, middle

# Questions & Demonstration

