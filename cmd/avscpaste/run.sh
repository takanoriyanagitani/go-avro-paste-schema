#!/bin/sh

filenames(){
	echo sample.d/input1.avsc
	echo sample.d/input2.avsc
}

pasted(){
	./avscpaste $( filenames )
}

pasted2yaml(){
	pasted |
		dasel \
			--read=json \
			--write=yaml |
		bat --language=yaml
}

pasted2yaml
#pasted
