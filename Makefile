all:
	cd build ; cmake --build . -- -j 64

normal:
	rm -rf build
	mkdir build
	cd build ; ../cmakung -DCMAKE_BUILD_TYPE=RelWithDebInfo .. ; cmake --build . -- -j 64


debug:
	rm -rf build
	mkdir build
	cd build ; ../cmakung -DCMAKE_BUILD_TYPE=Debug .. ; cmake --build . -- -j 64

asan:
	rm -rf build
	mkdir build
	cd build ; ../cmakung -DCMAKE_CXX_FLAGS="-fsanitize=address -fno-omit-frame-pointer" -DCMAKE_BUILD_TYPE=Debug .. ; cmake --build . -- -j 64

clean:
	rm -rf build

