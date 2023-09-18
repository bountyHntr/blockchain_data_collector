gen:
	abigen --abi=./smartcontract/erc20/erc20.abi --pkg=erc20 --out=smartcontract/erc20/erc20.go

build:
	go build -o ./xcollector

config = config.yaml
run:
	./xcollector --config=$(config) 2> `date +%s`.log