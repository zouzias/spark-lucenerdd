## Setup

# Add `.credentials` file under `~/.sbt/` folder with contents

```
realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user=(USERNAME)
password=(PASSWORD_HERE)
```

## Run sbt release to release signed both 2.10 and 2.11

```bash
sbt release
```

## Then, git checkout v0.X.X to the release tag first, and then type

```bash
sbt sonatypeRelease
```

## This will allow sonatype to release the artifacts to maven central.
## An alternative is to browse to https://oss.sonatype.org and do it manually
