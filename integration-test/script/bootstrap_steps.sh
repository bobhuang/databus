# here are the steps to start bootstrap manually
# they can be run as is or called separately
# stop can be called for 

# start the relay 
./dbus2_driver.py -c test_relay -o start

# generate events
./dbus2_gen_event.py

# reset the db and delete the checkpoints
./dbus2_driver.py -c bootstrap_dbreset

# start the producer
./dbus2_driver.py -c test_bootstrap_producer -o start

# start the bootstrap server on port 6060
./dbus2_driver.py -c bootstrap_server -o start

# start the bootstrap integrated consumer. Can be called multiple times.
./dbus2_driver.py -c bootstrap_consumer -o start

