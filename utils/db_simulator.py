import sqlite3
import pandas as pd
import random
import datetime
from datetime import datetime, timedelta
import json

DB_PATH = "data/sample_db.sqlite"

def setup_sample_db():
    """创建包含丰富数据的复杂样本数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 清理现有表
    tables_to_drop = [
        'marketing_campaigns', 'campaign_interactions', 'customer_segments',
        'inventory_logs', 'supplier_products', 'suppliers', 'product_reviews',
        'website_sessions', 'customer_support_tickets', 'employee_performance',
        'sales_targets', 'regional_performance', 'product_categories',
        'order_items', 'orders', 'products', 'customers', 'employees', 'departments'
    ]
    
    for table in tables_to_drop:
        cursor.execute(f"DROP TABLE IF EXISTS {table};")

    # 1. 核心业务表 - 部门和员工
    cursor.execute("""
        CREATE TABLE departments (
            department_id INTEGER PRIMARY KEY,
            department_name TEXT NOT NULL,
            manager_id INTEGER,
            budget REAL,
            location TEXT,
            created_date TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            department_id INTEGER,
            position TEXT,
            salary REAL,
            hire_date TEXT,
            performance_score REAL,
            manager_id INTEGER,
            status TEXT DEFAULT 'active',
            FOREIGN KEY(department_id) REFERENCES departments(department_id),
            FOREIGN KEY(manager_id) REFERENCES employees(employee_id)
        );
    """)

    # 2. 产品和库存管理
    cursor.execute("""
        CREATE TABLE product_categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            parent_category_id INTEGER,
            description TEXT,
            created_date TEXT,
            FOREIGN KEY(parent_category_id) REFERENCES product_categories(category_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE suppliers (
            supplier_id INTEGER PRIMARY KEY,
            supplier_name TEXT NOT NULL,
            contact_email TEXT,
            contact_phone TEXT,
            address TEXT,
            country TEXT,
            rating REAL,
            established_date TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            sku TEXT UNIQUE,
            category_id INTEGER,
            price REAL,
            cost REAL,
            weight REAL,
            dimensions TEXT,
            description TEXT,
            brand TEXT,
            launch_date TEXT,
            status TEXT DEFAULT 'active',
            supplier_id INTEGER,
            FOREIGN KEY(category_id) REFERENCES product_categories(category_id),
            FOREIGN KEY(supplier_id) REFERENCES suppliers(supplier_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE inventory_logs (
            log_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            change_type TEXT, -- 'purchase', 'sale', 'adjustment', 'return'
            quantity_change INTEGER,
            current_stock INTEGER,
            unit_cost REAL,
            timestamp TEXT,
            employee_id INTEGER,
            notes TEXT,
            FOREIGN KEY(product_id) REFERENCES products(product_id),
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
        );
    """)

    # 3. 客户和细分管理
    cursor.execute("""
        CREATE TABLE customer_segments (
            segment_id INTEGER PRIMARY KEY,
            segment_name TEXT NOT NULL,
            description TEXT,
            min_order_value REAL,
            min_order_count INTEGER,
            created_date TEXT
        );
    """)

    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            phone TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            country TEXT,
            postal_code TEXT,
            date_of_birth TEXT,
            gender TEXT,
            signup_date TEXT,
            last_login TEXT,
            segment_id INTEGER,
            lifetime_value REAL DEFAULT 0,
            acquisition_channel TEXT,
            status TEXT DEFAULT 'active',
            FOREIGN KEY(segment_id) REFERENCES customer_segments(segment_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE website_sessions (
            session_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            session_start TEXT,
            session_end TEXT,
            page_views INTEGER,
            bounce_rate REAL,
            device_type TEXT,
            browser TEXT,
            traffic_source TEXT,
            conversion_flag INTEGER DEFAULT 0,
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
        );
    """)

    # 4. 订单和销售
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            employee_id INTEGER, -- 销售员工
            order_date TEXT,
            ship_date TEXT,
            delivery_date TEXT,
            order_status TEXT, -- 'pending', 'processing', 'shipped', 'delivered', 'cancelled'
            payment_method TEXT,
            shipping_cost REAL,
            tax_amount REAL,
            discount_amount REAL,
            total_amount REAL,
            region TEXT,
            sales_channel TEXT, -- 'online', 'retail', 'phone', 'b2b'
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            unit_price REAL,
            discount_rate REAL DEFAULT 0,
            line_total REAL,
            FOREIGN KEY(order_id) REFERENCES orders(order_id),
            FOREIGN KEY(product_id) REFERENCES products(product_id)
        );
    """)

    # 5. 产品评价和反馈
    cursor.execute("""
        CREATE TABLE product_reviews (
            review_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            customer_id INTEGER,
            order_id INTEGER,
            rating INTEGER, -- 1-5 stars
            review_text TEXT,
            review_date TEXT,
            helpful_votes INTEGER DEFAULT 0,
            verified_purchase INTEGER DEFAULT 1,
            FOREIGN KEY(product_id) REFERENCES products(product_id),
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY(order_id) REFERENCES orders(order_id)
        );
    """)

    # 6. 客户服务
    cursor.execute("""
        CREATE TABLE customer_support_tickets (
            ticket_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            employee_id INTEGER, -- 处理员工
            category TEXT, -- 'technical', 'billing', 'shipping', 'product', 'other'
            priority TEXT, -- 'low', 'medium', 'high', 'urgent'
            status TEXT, -- 'open', 'in_progress', 'resolved', 'closed'
            subject TEXT,
            description TEXT,
            created_date TEXT,
            resolved_date TEXT,
            satisfaction_score INTEGER, -- 1-5
            resolution_time_hours REAL,
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id),
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
        );
    """)

    # 7. 营销活动
    cursor.execute("""
        CREATE TABLE marketing_campaigns (
            campaign_id INTEGER PRIMARY KEY,
            campaign_name TEXT NOT NULL,
            campaign_type TEXT, -- 'email', 'social', 'search', 'display', 'direct_mail'
            start_date TEXT,
            end_date TEXT,
            budget REAL,
            target_audience TEXT,
            goal TEXT,
            status TEXT, -- 'draft', 'active', 'paused', 'completed'
            employee_id INTEGER, -- 负责人
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE campaign_interactions (
            interaction_id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            customer_id INTEGER,
            interaction_type TEXT, -- 'email_open', 'click', 'conversion', 'unsubscribe'
            interaction_date TEXT,
            value REAL, -- 转化金额
            FOREIGN KEY(campaign_id) REFERENCES marketing_campaigns(campaign_id),
            FOREIGN KEY(customer_id) REFERENCES customers(customer_id)
        );
    """)

    # 8. 销售目标和绩效
    cursor.execute("""
        CREATE TABLE sales_targets (
            target_id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            year INTEGER,
            quarter INTEGER,
            target_amount REAL,
            actual_amount REAL DEFAULT 0,
            product_category TEXT,
            region TEXT,
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id)
        );
    """)

    cursor.execute("""
        CREATE TABLE employee_performance (
            performance_id INTEGER PRIMARY KEY,
            employee_id INTEGER,
            evaluation_date TEXT,
            overall_score REAL, -- 1-10
            communication_score REAL,
            technical_score REAL,
            leadership_score REAL,
            goals_achieved INTEGER,
            goals_total INTEGER,
            feedback TEXT,
            evaluator_id INTEGER,
            FOREIGN KEY(employee_id) REFERENCES employees(employee_id),
            FOREIGN KEY(evaluator_id) REFERENCES employees(employee_id)
        );
    """)

    # 9. 地区销售表现
    cursor.execute("""
        CREATE TABLE regional_performance (
            region_id INTEGER PRIMARY KEY,
            region_name TEXT NOT NULL,
            country TEXT,
            population INTEGER,
            gdp_per_capita REAL,
            market_size REAL,
            competition_level TEXT, -- 'low', 'medium', 'high'
            sales_manager_id INTEGER,
            FOREIGN KEY(sales_manager_id) REFERENCES employees(employee_id)
        );
    """)

    # 开始插入模拟数据
    print("开始生成模拟数据...")

    # 插入部门数据
    departments_data = [
        (1, 'Sales', None, 500000.0, 'New York', '2020-01-01'),
        (2, 'Marketing', None, 300000.0, 'Los Angeles', '2020-01-01'),
        (3, 'Engineering', None, 800000.0, 'San Francisco', '2020-01-01'),
        (4, 'Customer Service', None, 200000.0, 'Austin', '2020-01-01'),
        (5, 'HR', None, 150000.0, 'Chicago', '2020-01-01'),
        (6, 'Finance', None, 250000.0, 'New York', '2020-01-01'),
        (7, 'Operations', None, 400000.0, 'Seattle', '2020-01-01'),
        (8, 'Product Management', None, 350000.0, 'San Francisco', '2020-01-01')
    ]
    cursor.executemany("INSERT INTO departments VALUES (?, ?, ?, ?, ?, ?);", departments_data)

    # 插入员工数据（扩展到50个员工）
    employees_data = []
    positions = {
        1: ['Sales Rep', 'Senior Sales Rep', 'Sales Manager', 'VP Sales'],
        2: ['Marketing Specialist', 'Marketing Manager', 'CMO'],
        3: ['Software Engineer', 'Senior Engineer', 'Tech Lead', 'Engineering Manager', 'CTO'],
        4: ['Support Agent', 'Senior Support Agent', 'Support Manager'],
        5: ['HR Specialist', 'HR Manager', 'CHRO'],
        6: ['Financial Analyst', 'Senior Analyst', 'Finance Manager', 'CFO'],
        7: ['Operations Specialist', 'Operations Manager', 'COO'],
        8: ['Product Manager', 'Senior Product Manager', 'VP Product']
    }
    
    names = ['Alice Johnson', 'Bob Smith', 'Charlie Brown', 'Diana Prince', 'Eve Wilson', 
            'Frank Miller', 'Grace Lee', 'Henry Davis', 'Iris Chen', 'Jack Wilson',
            'Kate Thompson', 'Leo Garcia', 'Maya Patel', 'Noah Williams', 'Olivia Martinez',
            'Peter Anderson', 'Quinn Taylor', 'Rachel Moore', 'Sam Jackson', 'Tina Liu',
            'Uma Sharma', 'Victor Kim', 'Wendy Chang', 'Xavier Rodriguez', 'Yuki Tanaka',
            'Zoe Clark', 'Aaron Lewis', 'Bella Ross', 'Carlos Mendez', 'Donna Wright',
            'Ethan Cooper', 'Fiona Murphy', 'George Hall', 'Hannah Green', 'Ian Foster',
            'Julia Barnes', 'Kevin Scott', 'Luna Wang', 'Max Turner', 'Nina Patel',
            'Oscar Martinez', 'Penny Johnson', 'Quincy Adams', 'Rita Singh', 'Steve Wilson',
            'Tara Chen', 'Ulysses Grant', 'Vera Kozlov', 'Walter White', 'Xena Warrior']

    for i in range(50):
        dept_id = (i % 8) + 1
        position = random.choice(positions[dept_id])
        base_salary = {'Sales Rep': 45000, 'Senior Sales Rep': 65000, 'Sales Manager': 85000, 'VP Sales': 150000,
                      'Marketing Specialist': 50000, 'Marketing Manager': 75000, 'CMO': 180000,
                      'Software Engineer': 80000, 'Senior Engineer': 120000, 'Tech Lead': 140000, 
                      'Engineering Manager': 160000, 'CTO': 200000,
                      'Support Agent': 35000, 'Senior Support Agent': 45000, 'Support Manager': 65000,
                      'HR Specialist': 45000, 'HR Manager': 70000, 'CHRO': 160000,
                      'Financial Analyst': 55000, 'Senior Analyst': 75000, 'Finance Manager': 95000, 'CFO': 180000,
                      'Operations Specialist': 50000, 'Operations Manager': 80000, 'COO': 170000,
                      'Product Manager': 90000, 'Senior Product Manager': 120000, 'VP Product': 160000}
        
        hire_date = datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2000))
        performance = round(random.uniform(6.5, 9.5), 1)
        manager_id = None if 'VP' in position or 'C' in position[:2] else random.randint(1, max(1, i-5))
        
        employees_data.append((
            i + 1, names[i], f"{names[i].lower().replace(' ', '.')}@company.com",
            dept_id, position, base_salary.get(position, 50000) + random.randint(-5000, 15000),
            hire_date.strftime('%Y-%m-%d'), performance, manager_id, 'active'
        ))
    
    cursor.executemany("INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", employees_data)

    # 插入产品分类
    categories_data = [
        (1, 'Electronics', None, 'Electronic devices and accessories', '2020-01-01'),
        (2, 'Smartphones', 1, 'Mobile phones and accessories', '2020-01-01'),
        (3, 'Laptops', 1, 'Portable computers', '2020-01-01'),
        (4, 'Tablets', 1, 'Touch screen tablets', '2020-01-01'),
        (5, 'Home & Garden', None, 'Home improvement and gardening', '2020-01-01'),
        (6, 'Kitchen', 5, 'Kitchen appliances and tools', '2020-01-01'),
        (7, 'Furniture', 5, 'Home furniture', '2020-01-01'),
        (8, 'Clothing', None, 'Fashion and apparel', '2020-01-01'),
        (9, 'Men\'s Clothing', 8, 'Men\'s fashion', '2020-01-01'),
        (10, 'Women\'s Clothing', 8, 'Women\'s fashion', '2020-01-01'),
        (11, 'Sports & Fitness', None, 'Sports equipment and fitness gear', '2020-01-01'),
        (12, 'Books', None, 'Physical and digital books', '2020-01-01')
    ]
    cursor.executemany("INSERT INTO product_categories VALUES (?, ?, ?, ?, ?);", categories_data)

    # 插入供应商
    suppliers_data = [
        (1, 'TechCorp Solutions', 'contact@techcorp.com', '555-0101', '123 Tech St, Silicon Valley, CA', 'USA', 4.5, '2015-03-15'),
        (2, 'Global Electronics Ltd', 'info@globalelec.com', '555-0102', '456 Circuit Ave, Shenzhen', 'China', 4.2, '2012-08-20'),
        (3, 'Fashion Forward Inc', 'orders@fashionforward.com', '555-0103', '789 Style Blvd, New York, NY', 'USA', 4.7, '2018-01-10'),
        (4, 'Home Comfort Co', 'sales@homecomfort.com', '555-0104', '321 Cozy Lane, Portland, OR', 'USA', 4.3, '2016-05-25'),
        (5, 'SportMax International', 'wholesale@sportmax.com', '555-0105', '654 Athletic Dr, Munich', 'Germany', 4.6, '2014-11-30'),
        (6, 'BookWorld Publishers', 'distribution@bookworld.com', '555-0106', '987 Literary St, London', 'UK', 4.4, '2013-07-12')
    ]
    cursor.executemany("INSERT INTO suppliers VALUES (?, ?, ?, ?, ?, ?, ?, ?);", suppliers_data)

    # 生成产品数据（200个产品）
    products_data = []
    product_names = {
        2: ['iPhone 15 Pro', 'Samsung Galaxy S24', 'Google Pixel 8', 'OnePlus 12', 'Xiaomi Mi 14'],
        3: ['MacBook Pro M3', 'Dell XPS 13', 'HP Spectre x360', 'Lenovo ThinkPad X1', 'ASUS ZenBook'],
        4: ['iPad Pro', 'Samsung Galaxy Tab S9', 'Microsoft Surface Pro', 'Amazon Fire HD', 'Lenovo Tab P11'],
        6: ['KitchenAid Mixer', 'Ninja Blender', 'Instant Pot', 'Cuisinart Food Processor', 'Vitamix Blender'],
        7: ['Ergonomic Office Chair', 'Standing Desk', 'Bookshelf', 'Dining Table', 'Sofa Set'],
        9: ['Men\'s Casual Shirt', 'Business Suit', 'Jeans', 'Polo Shirt', 'Winter Jacket'],
        10: ['Summer Dress', 'Blouse', 'Leggings', 'Evening Gown', 'Casual Jacket'],
        11: ['Yoga Mat', 'Dumbbells', 'Treadmill', 'Resistance Bands', 'Exercise Bike'],
        12: ['Programming Guide', 'Business Strategy', 'Fiction Novel', 'History Book', 'Self-Help']
    }

    for i in range(200):
        category_id = random.choice([2, 3, 4, 6, 7, 9, 10, 11, 12])
        base_names = product_names[category_id]
        product_name = random.choice(base_names) + f" - Model {i+1}"
        
        # 价格基于分类
        price_ranges = {2: (200, 1200), 3: (500, 2500), 4: (150, 800), 6: (50, 400),
                       7: (100, 1500), 9: (20, 200), 10: (25, 300), 11: (15, 800), 12: (10, 50)}
        min_price, max_price = price_ranges[category_id]
        price = round(random.uniform(min_price, max_price), 2)
        cost = round(price * random.uniform(0.4, 0.7), 2)
        
        launch_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))
        supplier_id = random.randint(1, 6)
        
        products_data.append((
            i + 1, product_name, f"SKU{i+1:04d}", category_id, price, cost,
            round(random.uniform(0.1, 5.0), 2), f"{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,30)}cm",
            f"High-quality {product_name.split()[0]} with premium features", 
            random.choice(['Apple', 'Samsung', 'Google', 'Nike', 'Adidas', 'Sony', 'LG', 'Generic']),
            launch_date.strftime('%Y-%m-%d'), 'active', supplier_id
        ))
    
    cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", products_data)

    # 插入客户细分
    segments_data = [
        (1, 'VIP', 'High-value repeat customers', 1000.0, 5, '2020-01-01'),
        (2, 'Regular', 'Regular customers', 100.0, 2, '2020-01-01'),
        (3, 'New', 'New customers', 0.0, 0, '2020-01-01'),
        (4, 'At Risk', 'Customers at risk of churning', 50.0, 1, '2020-01-01')
    ]
    cursor.executemany("INSERT INTO customer_segments VALUES (?, ?, ?, ?, ?, ?);", segments_data)

    # 生成客户数据（1000个客户）
    customers_data = []
    first_names = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 'William', 'Elizabeth',
                  'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica', 'Thomas', 'Sarah', 'Christopher', 'Karen',
                  'Daniel', 'Nancy', 'Matthew', 'Lisa', 'Anthony', 'Betty', 'Mark', 'Helen', 'Donald', 'Sandra']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez',
                 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose']
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA']
    countries = ['USA', 'Canada', 'UK', 'Australia', 'Germany', 'France', 'Japan', 'Brazil']
    channels = ['organic_search', 'paid_search', 'social_media', 'email', 'direct', 'referral', 'affiliate']

    for i in range(1000):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        name = f"{first_name} {last_name}"
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
        
        signup_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
        last_login = signup_date + timedelta(days=random.randint(0, 300))
        
        city_idx = random.randint(0, 9)
        segment_id = random.choices([1, 2, 3, 4], weights=[5, 40, 35, 20])[0]
        
        customers_data.append((
            i + 1, name, email, f"555-{random.randint(1000,9999)}", 
            f"{random.randint(100,9999)} {random.choice(['Main', 'Oak', 'First', 'Second', 'Park', 'Elm'])} St",
            cities[city_idx], states[city_idx], random.choice(countries), f"{random.randint(10000,99999)}",
            (datetime(1950, 1, 1) + timedelta(days=random.randint(0, 25000))).strftime('%Y-%m-%d'),
            random.choice(['M', 'F', 'Other']), signup_date.strftime('%Y-%m-%d'),
            last_login.strftime('%Y-%m-%d'), segment_id, round(random.uniform(0, 5000), 2),
            random.choice(channels), random.choice(['active', 'inactive'])
        ))
    
    cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", customers_data)

    print("基础数据插入完成，开始生成交易数据...")

    # 生成订单数据（5000个订单，覆盖2年时间）
    orders_data = []
    for i in range(5000):
        customer_id = random.randint(1, 1000)
        employee_id = random.choice([j for j in range(1, 51) if employees_data[j-1][4] in ['Sales Rep', 'Senior Sales Rep']])
        
        order_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
        ship_date = order_date + timedelta(days=random.randint(1, 5))
        delivery_date = ship_date + timedelta(days=random.randint(1, 10))
        
        status = random.choices(['delivered', 'shipped', 'processing', 'cancelled'], weights=[70, 15, 10, 5])[0]
        payment_method = random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer'])
        shipping_cost = round(random.uniform(5, 25), 2)
        discount_amount = round(random.uniform(0, 50), 2)
        
        # 总金额稍后计算
        region = random.choice(['North', 'South', 'East', 'West', 'Central'])
        channel = random.choice(['online', 'retail', 'phone', 'b2b'])
        
        orders_data.append((
            i + 1, customer_id, employee_id, order_date.strftime('%Y-%m-%d'),
            ship_date.strftime('%Y-%m-%d') if status != 'cancelled' else None,
            delivery_date.strftime('%Y-%m-%d') if status == 'delivered' else None,
            status, payment_method, shipping_cost, 0, discount_amount, 0, region, channel
        ))
    
    cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", orders_data)

    # 生成订单明细数据
    order_items_data = []
    item_id = 1
    for order_id in range(1, 5001):
        # 每个订单1-5个商品
        num_items = random.randint(1, 5)
        order_total = 0
        
        for _ in range(num_items):
            product_id = random.randint(1, 200)
            quantity = random.randint(1, 3)
            # 获取产品价格
            unit_price = products_data[product_id - 1][4]  # price字段
            discount_rate = random.uniform(0, 0.2)
            line_total = round(unit_price * quantity * (1 - discount_rate), 2)
            order_total += line_total
            
            order_items_data.append((
                item_id, order_id, product_id, quantity, unit_price, 
                round(discount_rate, 3), line_total
            ))
            item_id += 1
        
        # 更新订单总金额
        tax_amount = round(order_total * 0.08, 2)
        cursor.execute("UPDATE orders SET tax_amount = ?, total_amount = ? WHERE order_id = ?", 
                      (tax_amount, order_total + tax_amount + orders_data[order_id-1][8] - orders_data[order_id-1][10], order_id))
    
    cursor.executemany("INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?, ?);", order_items_data)

    print("订单数据生成完成，继续生成其他业务数据...")

    # 生成产品评价数据
    reviews_data = []
    for i in range(2000):
        # 只对已交付的订单生成评价
        order_id = random.randint(1, 5000)
        # 获取订单信息
        cursor.execute("SELECT customer_id FROM orders WHERE order_id = ? AND order_status = 'delivered'", (order_id,))
        result = cursor.fetchone()
        if result:
            customer_id = result[0]
            product_id = random.randint(1, 200)
            rating = random.choices([1, 2, 3, 4, 5], weights=[5, 10, 15, 30, 40])[0]
            
            review_texts = [
                "Great product, highly recommend!",
                "Good value for money",
                "Excellent quality and fast shipping",
                "Not what I expected, but okay",
                "Outstanding product, will buy again",
                "Poor quality, disappointed",
                "Average product, nothing special",
                "Perfect for my needs",
                "Could be better for the price",
                "Exactly as described, very happy"
            ]
            
            review_date = (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d')
            
            reviews_data.append((
                i + 1, product_id, customer_id, order_id, rating,
                random.choice(review_texts), review_date, random.randint(0, 50), 1
            ))
    
    cursor.executemany("INSERT INTO product_reviews VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);", reviews_data)

    # 生成网站会话数据
    sessions_data = []
    for i in range(10000):
        customer_id = random.choice([None] + list(range(1, 1001)))  # 包含匿名用户
        session_start = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730), 
                                                        hours=random.randint(0, 23), 
                                                        minutes=random.randint(0, 59))
        session_duration = random.randint(30, 3600)  # 30秒到1小时
        session_end = session_start + timedelta(seconds=session_duration)
        
        page_views = random.randint(1, 20)
        bounce_rate = 1.0 if page_views == 1 else 0.0
        device_type = random.choice(['desktop', 'mobile', 'tablet'])
        browser = random.choice(['Chrome', 'Firefox', 'Safari', 'Edge'])
        traffic_source = random.choice(['organic', 'paid_search', 'social', 'direct', 'email', 'referral'])
        conversion_flag = random.choices([0, 1], weights=[85, 15])[0]
        
        sessions_data.append((
            i + 1, customer_id, session_start.strftime('%Y-%m-%d %H:%M:%S'),
            session_end.strftime('%Y-%m-%d %H:%M:%S'), page_views, bounce_rate,
            device_type, browser, traffic_source, conversion_flag
        ))
    
    cursor.executemany("INSERT INTO website_sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", sessions_data)

    # 生成客服工单数据
    tickets_data = []
    categories = ['technical', 'billing', 'shipping', 'product', 'other']
    priorities = ['low', 'medium', 'high', 'urgent']
    statuses = ['resolved', 'closed', 'open', 'in_progress']
    
    for i in range(1500):
        customer_id = random.randint(1, 1000)
        employee_id = random.choice([j for j in range(1, 51) if employees_data[j-1][3] == 4])  # Customer Service
        category = random.choice(categories)
        priority = random.choices(priorities, weights=[40, 35, 20, 5])[0]
        status = random.choices(statuses, weights=[50, 30, 15, 5])[0]
        
        subjects = {
            'technical': ['Website not loading', 'Login issues', 'Mobile app crash', 'Payment processing error'],
            'billing': ['Incorrect charge', 'Refund request', 'Payment method update', 'Invoice inquiry'],
            'shipping': ['Delayed delivery', 'Wrong address', 'Damaged package', 'Tracking issues'],
            'product': ['Defective item', 'Missing parts', 'Wrong size', 'Product inquiry'],
            'other': ['General inquiry', 'Feedback', 'Complaint', 'Suggestion']
        }
        
        created_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
        resolution_time = random.uniform(1, 120) if status in ['resolved', 'closed'] else None
        resolved_date = (created_date + timedelta(hours=resolution_time)).strftime('%Y-%m-%d %H:%M:%S') if resolution_time else None
        satisfaction_score = random.randint(1, 5) if status in ['resolved', 'closed'] else None
        
        tickets_data.append((
            i + 1, customer_id, employee_id, category, priority, status,
            random.choice(subjects[category]), f"Customer issue regarding {category}",
            created_date.strftime('%Y-%m-%d %H:%M:%S'), resolved_date,
            satisfaction_score, resolution_time
        ))
    
    cursor.executemany("INSERT INTO customer_support_tickets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);", tickets_data)

    print("所有数据生成完成！")
    
    conn.commit()
    conn.close()

def initialize_database():
    """初始化数据库的别名函数"""
    setup_sample_db()

def run_query(query):
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df.head().to_string(index=False)
    except Exception as e:
        return f"Query failed: {e}"

def get_db_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    schema = ""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name, in tables:
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        create_stmt = cursor.fetchone()[0]
        schema += create_stmt + ";\n\n"
    conn.close()
    return schema

def get_structured_schema(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    lines = ["📊 DataCrew AutoPilot 企业级数据分析数据库", ""]
    lines.append("🏢 核心业务表:")
    lines.append("- departments: department_id, department_name, manager_id, budget, location, created_date")
    lines.append("- employees: employee_id, name, email, department_id, position, salary, hire_date, performance_score, manager_id, status")
    lines.append("")
    lines.append("🛍️ 产品与库存:")
    lines.append("- product_categories: category_id, category_name, parent_category_id, description, created_date")
    lines.append("- suppliers: supplier_id, supplier_name, contact_email, contact_phone, address, country, rating, established_date")
    lines.append("- products: product_id, product_name, sku, category_id, price, cost, weight, dimensions, description, brand, launch_date, status, supplier_id")
    lines.append("- inventory_logs: log_id, product_id, change_type, quantity_change, current_stock, unit_cost, timestamp, employee_id, notes")
    lines.append("")
    lines.append("👥 客户关系管理:")
    lines.append("- customer_segments: segment_id, segment_name, description, min_order_value, min_order_count, created_date")
    lines.append("- customers: customer_id, name, email, phone, address, city, state, country, postal_code, date_of_birth, gender, signup_date, last_login, segment_id, lifetime_value, acquisition_channel, status")
    lines.append("- website_sessions: session_id, customer_id, session_start, session_end, page_views, bounce_rate, device_type, browser, traffic_source, conversion_flag")
    lines.append("")
    lines.append("🛒 订单与销售:")
    lines.append("- orders: order_id, customer_id, employee_id, order_date, ship_date, delivery_date, order_status, payment_method, shipping_cost, tax_amount, discount_amount, total_amount, region, sales_channel")
    lines.append("- order_items: order_item_id, order_id, product_id, quantity, unit_price, discount_rate, line_total")
    lines.append("")
    lines.append("⭐ 产品反馈:")
    lines.append("- product_reviews: review_id, product_id, customer_id, order_id, rating, review_text, review_date, helpful_votes, verified_purchase")
    lines.append("")
    lines.append("🎯 客户服务:")
    lines.append("- customer_support_tickets: ticket_id, customer_id, employee_id, category, priority, status, subject, description, created_date, resolved_date, satisfaction_score, resolution_time_hours")
    lines.append("")
    lines.append("📈 营销活动:")
    lines.append("- marketing_campaigns: campaign_id, campaign_name, campaign_type, start_date, end_date, budget, target_audience, goal, status, employee_id")
    lines.append("- campaign_interactions: interaction_id, campaign_id, customer_id, interaction_type, interaction_date, value")
    lines.append("")
    lines.append("🎯 销售绩效:")
    lines.append("- sales_targets: target_id, employee_id, year, quarter, target_amount, actual_amount, product_category, region")
    lines.append("- employee_performance: performance_id, employee_id, evaluation_date, overall_score, communication_score, technical_score, leadership_score, goals_achieved, goals_total, feedback, evaluator_id")
    lines.append("- regional_performance: region_id, region_name, country, population, gdp_per_capita, market_size, competition_level, sales_manager_id")
    lines.append("")
    lines.append("📊 数据规模统计:")
    
    # 获取每个表的行数
    for table_name, in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        lines.append(f"- {table_name}: {count:,} 条记录")
    
    conn.close()
    return '\n'.join(lines)

if __name__ == "__main__":
    setup_sample_db()
    print("复杂企业级样本数据库创建完成！")
    print("包含以下数据：")
    print("- 8个部门，50名员工")
    print("- 12个产品分类，200个产品")
    print("- 6个供应商")
    print("- 4个客户细分，1000个客户")
    print("- 5000个订单，约15000个订单明细")
    print("- 2000条产品评价")
    print("- 10000次网站会话")
    print("- 1500个客服工单")
    print("- 丰富的营销和绩效数据")
