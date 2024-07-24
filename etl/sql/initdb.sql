-- Создание базы данных bankdb и пользователя neoflex

CREATE DATABASE bankdb;

CREATE ROLE neouser
LOGIN
SUPERUSER
password 'neoflex';