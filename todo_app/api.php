<?php
header("Content-Type: application/json");
require_once 'db.php';

$method = $_SERVER['REQUEST_METHOD'];

// Get JSON input
$input = json_decode(file_get_contents('php://input'), true);

switch ($method) {
    case 'GET':
        $stmt = $pdo->query("SELECT * FROM todos ORDER BY created_at DESC");
        echo json_encode($stmt->fetchAll());
        break;

    case 'POST':
        if (!empty($input['title'])) {
            $stmt = $pdo->prepare("INSERT INTO todos (title) VALUES (:title)");
            $stmt->execute(['title' => htmlspecialchars($input['title'])]);
            echo json_encode(["id" => $pdo->lastInsertId(), "title" => $input['title'], "is_completed" => false]);
        } else {
            echo json_encode(["error" => "Title is required"]);
        }
        break;

    case 'PUT':
        if (isset($input['id']) && isset($input['is_completed'])) {
            $stmt = $pdo->prepare("UPDATE todos SET is_completed = :is_completed WHERE id = :id");
            $stmt->execute([
                'is_completed' => $input['is_completed'] ? 1 : 0,
                'id' => $input['id']
            ]);
            echo json_encode(["success" => true]);
        }
        break;

    case 'DELETE':
        if (isset($_GET['id'])) {
            $stmt = $pdo->prepare("DELETE FROM todos WHERE id = :id");
            $stmt->execute(['id' => $_GET['id']]);
            echo json_encode(["success" => true]);
        }
        break;
}
?>
