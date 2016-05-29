#!/bin/bash

# git checkout v0.X.X first, and then type

sbt +publishSigned
sbt sonatypeRelease
