<?php
require 'db.php';

try {
    $sql = "CREATE TABLE IF NOT EXISTS todos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        is_completed BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )";
    $pdo->exec($sql);
    echo "SUCCESS: The 'todos' table was created successfully!";
} catch (PDOException $e) {
    echo "ERROR: " . $e->getMessage();
}
?>
