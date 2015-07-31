get_abs_script_path() {
pushd . >/dev/null
cd $(dirname $0)
dir=$(pwd)
popd  >/dev/null
}

get_abs_script_path

echo "Deploying twitter insights..."
$dir/main-twitter-insights.sh
