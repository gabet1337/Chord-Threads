Chord-Threads
=============

Implementation of the Chord protocol using Sockets.

Receiving and sending messages will be done using queues.

Issues:
=======

The shutdown / leaveGroup procedure sucks. It needs to be cleaned up, and implemented properly.

Synchronization of joins

Passing of objects when joining

BUGS:
=====

leaveGroup should block its successor inorder for migrate to work

lookup blocking should be tested

migrate has a bug when joining or leaving

no synchronization on joins and leaves

shutting down threads

busy/waiting should be replaced with wait/notity

closing down the chordServer - dont close connection promptly.