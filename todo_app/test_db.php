<?php
error_reporting(E_ALL);
ini_set('display_errors', 1);

$host = '127.0.0.1'; // or localhost
$db   = 'to_do_fqyh6e'; 
$user = 'to_do_fqyh6e'; 
$pass = 'TodoPassword123!'; 
$charset = 'utf8mb4';

$dsn = "mysql:host=$host;dbname=$db;charset=$charset";
$options = [
    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    PDO::ATTR_EMULATE_PREPARES   => false,
];

try {
    $pdo = new PDO($dsn, $user, $pass, $options);
    echo "DB Connection SUCCESS. <br>";
    
    $stmt = $pdo->query("SELECT 1 FROM todos LIMIT 1");
    echo "Table 'todos' exists. <br>";
} catch (\PDOException $e) {
    echo "DB ERROR: " . $e->getMessage();
} catch (Exception $e) {
    echo "GENERAL ERROR: " . $e->getMessage();
}
?>
