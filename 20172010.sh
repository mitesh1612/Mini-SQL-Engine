if [[ "$#" -ne 1 ]]; then
	echo "Illegal Number of Parameters!"
	exit 1
fi
python Engine.py "$1"
