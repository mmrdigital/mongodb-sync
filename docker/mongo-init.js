// MongoDB initialization script for Docker
// This script sets up replica sets and creates sample data for testing

// Initialize replica set
rs.initiate({
  _id: "rs0",
  version: 1,
  members: [
    { _id: 0, host: "localhost:27017", priority: 1 }
  ]
});

// Wait for replica set to be ready
sleep(2000);

// Switch to the test database
db = db.getSiblingDB('testdb');

// Create sample collections and data for testing
db.users.insertMany([
  {
    _id: ObjectId(),
    username: "john_doe",
    email: "john@example.com",
    createdAt: new Date(),
    profile: {
      firstName: "John",
      lastName: "Doe",
      age: 30
    }
  },
  {
    _id: ObjectId(),
    username: "jane_smith", 
    email: "jane@example.com",
    createdAt: new Date(),
    profile: {
      firstName: "Jane",
      lastName: "Smith",
      age: 28
    }
  }
]);

db.products.insertMany([
  {
    _id: ObjectId(),
    name: "Laptop Computer",
    price: 999.99,
    category: "Electronics",
    stock: 50,
    createdAt: new Date()
  },
  {
    _id: ObjectId(),
    name: "Coffee Mug",
    price: 14.99,
    category: "Kitchen",
    stock: 200,
    createdAt: new Date()
  }
]);

db.orders.insertMany([
  {
    _id: ObjectId(),
    userId: db.users.findOne({username: "john_doe"})._id,
    items: [
      {
        productId: db.products.findOne({name: "Laptop Computer"})._id,
        quantity: 1,
        price: 999.99
      }
    ],
    total: 999.99,
    status: "completed",
    createdAt: new Date()
  }
]);

// Create indexes for better performance
db.users.createIndex({ "email": 1 }, { unique: true });
db.users.createIndex({ "username": 1 }, { unique: true });
db.products.createIndex({ "category": 1 });
db.products.createIndex({ "name": "text" });
db.orders.createIndex({ "userId": 1 });
db.orders.createIndex({ "status": 1 });

print("MongoDB initialization completed successfully!");