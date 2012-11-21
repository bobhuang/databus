lib_dir="$script_dir/../lib"
conf_dir="$script_dir/../conf"

cp="."
for f in ${lib_dir}/*.jar ; do
  cp="${cp}:${f}"
done
