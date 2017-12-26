# ArrowExample
Java read and write example for Apache Arrow

For more details please see 
[https://github.com/animeshtrivedi/animeshtrivedi.github.io/blob/master/blog/2017-12-26-arrow.md](https://github.com/animeshtrivedi/animeshtrivedi.github.io/blob/master/blog/2017-12-26-arrow.md)

## How to run 

Run `build-single-assembly.sh` to make a single uber jar containing 
all classes. 

To write a file run `run-example.sh`. This will generate random 
data and write it to `./example.arrow` file. 

Then run `run-read.sh`, this will read the file and display the 
data.