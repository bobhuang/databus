lib_dir="$script_dir/../lib"

cp="."
for f in ${lib_dir}/*.jar ; do
# note evil, evil hack to build classpath in reverse order so that the newer version of jars hide the older versions rather than vice versa
# we ran into this because of circular espresso -> databus2 -> espresso versions where we have older espresso jars in the classpath in addition to current version
  cp="${f}:${cp}"
done
