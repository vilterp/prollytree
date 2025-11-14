#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

echo "Creating sample SQLite database..."

# Create a test SQLite database
sqlite3 /tmp/test_data.db << 'EOF'
-- Users table with single primary key
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    email TEXT,
    created_at TEXT
);

INSERT INTO users (username, email, created_at) VALUES
    ('alice', 'alice@example.com', '2024-01-01'),
    ('bob', 'bob@example.com', '2024-01-02'),
    ('charlie', 'charlie@example.com', '2024-01-03'),
    ('diana', 'diana@example.com', '2024-01-04');

-- Products table with composite key
CREATE TABLE products (
    category TEXT,
    product_id INTEGER,
    name TEXT,
    price REAL,
    PRIMARY KEY (category, product_id)
);

INSERT INTO products (category, product_id, name, price) VALUES
    ('electronics', 1, 'Laptop', 999.99),
    ('electronics', 2, 'Mouse', 29.99),
    ('electronics', 3, 'Keyboard', 79.99),
    ('books', 1, 'Database Systems', 59.99),
    ('books', 2, 'Distributed Systems', 69.99);

-- Orders table without explicit primary key (will use rowid)
CREATE TABLE orders (
    user_id INTEGER,
    product_category TEXT,
    product_id INTEGER,
    quantity INTEGER,
    order_date TEXT
);

INSERT INTO orders VALUES
    (1, 'electronics', 1, 1, '2024-02-01'),
    (1, 'books', 1, 2, '2024-02-02'),
    (2, 'electronics', 2, 1, '2024-02-03'),
    (3, 'books', 2, 1, '2024-02-04');
EOF

echo "Sample database created at /tmp/test_data.db"
echo ""
echo "Database contents:"
echo "=================="
sqlite3 /tmp/test_data.db "SELECT 'Users: ' || COUNT(*) FROM users UNION ALL SELECT 'Products: ' || COUNT(*) FROM products UNION ALL SELECT 'Orders: ' || COUNT(*) FROM orders;"

echo ""
echo "Setting up S3 environment variables for LocalStack..."
export S3_BUCKET=prollytree-test
export S3_ENDPOINT_URL=http://127.0.0.1:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1

echo ""
echo "Running import script..."
echo "========================"
python /tmp/sqlite_to_s3_prolly.py \
    /tmp/test_data.db \
    $S3_BUCKET \
    --endpoint-url $S3_ENDPOINT_URL \
    --batch-size 100

echo ""
echo "Verifying import with AWS CLI..."
echo "================================="
aws --endpoint-url=$S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ 2>/dev/null | wc -l | xargs echo "Number of objects in S3:"

echo ""
echo "Sample object keys:"
aws --endpoint-url=$S3_ENDPOINT_URL s3 ls s3://$S3_BUCKET/ 2>/dev/null | head -10

echo ""
echo "Done! Test completed successfully."