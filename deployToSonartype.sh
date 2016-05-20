#!/bin/bash

sbt +publishSigned
sbt sonatypeRelease
