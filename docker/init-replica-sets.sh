#!/bin/bash
set -e

echo "Waiting for MongoDB instances to start..."
sleep 15

echo "Initializing source MongoDB replica set (rs0)..."
mongosh --host mongo-source:27017 --username admin --password password123 --authenticationDatabase admin --eval "
try {
    rs.status()
    console.log('Replica set rs0 already initialized')
} catch (e) {
    console.log('Initializing replica set rs0...')
    rs.initiate({
        _id: 'rs0',
        members: [
            { _id: 0, host: 'mongo-source:27017' }
        ]
    })
    console.log('Replica set rs0 initialized')
}
"

echo "Waiting for rs0 to become primary..."
sleep 10

echo "Initializing destination MongoDB replica set (rs1)..."
mongosh --host mongo-dest:27017 --username admin --password password123 --authenticationDatabase admin --eval "
try {
    rs.status()
    console.log('Replica set rs1 already initialized')
} catch (e) {
    console.log('Initializing replica set rs1...')
    rs.initiate({
        _id: 'rs1',
        members: [
            { _id: 0, host: 'mongo-dest:27017' }
        ]
    })
    console.log('Replica set rs1 initialized')
}
"

echo "Waiting for rs1 to become primary..."
sleep 10

echo "Replica sets initialization completed!"