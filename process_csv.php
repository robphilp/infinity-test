<?php

require_once __DIR__ . '/CSVProcessor.php';

const DB_HOST = '127.0.0.1';
const DB_PORT = '3306';
const DB_USER = 'root';
const DB_PASS = '';
const DB_NAME = 'infinity_test';

$dsn = sprintf('mysql:host=%s;port=%s;', DB_HOST, DB_PORT);
$pdo = new \PDO($dsn, DB_USER, DB_PASS);

$processor = new CSVProcessor($pdo, DB_NAME);
$processor->process();
