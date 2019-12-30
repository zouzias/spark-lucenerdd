## Setup

# Add `sonatype.sbt` file under `~/.sbt/1.0/` folder with contents

```
credentials += Credentials("Sonatype Nexus Repository Manager",
                           "oss.sonatype.org",
                           "zouzias",
                           "PASSWORD_HERE")
```

## Run sbt release to release signed both 2.10 and 2.11

```
sbt release
```

## Then, git checkout v0.X.X to the release tag first, and then type

```
sbt sonatypeRelease
```

## This will allow sonatype to release the artifacts to maven central.
## An alternative is to browse to https://oss.sonatype.org and do it manually
