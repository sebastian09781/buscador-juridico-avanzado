# ⚖️ Buscador Jurídico Avanzado

Herramienta de búsqueda diseñada para analizar grandes colecciones de documentos jurídicos y encontrar información relevante de forma rápida y eficiente.

Este proyecto permite **indexar y consultar documentos legales** (sentencias, relatorías, conceptos, entre otros) utilizando **búsqueda de texto completo con SQLite FTS5**, facilitando el trabajo de investigación jurídica.

Está pensado especialmente para **estudiantes de derecho, investigadores y operadores judiciales** que necesitan localizar información dentro de grandes repositorios documentales.

---

## ✨ Características

🔎 **Búsqueda de texto completo** dentro de documentos jurídicos
📚 **Indexación automática** de archivos dentro de una carpeta
⚡ **Resultados rápidos** gracias al motor FTS5 de SQLite
🖥️ **Interfaz gráfica sencilla** desarrollada en Tkinter
📂 **Análisis automático de documentos** sin abrirlos manualmente
🔧 **Instalación automática de dependencias** al iniciar el programa
🚀 Optimizado para **repositorios documentales grandes**

---

## 📄 Tipos de documentos soportados

El buscador puede analizar e indexar:

* 📑 **PDF**
* 📝 **DOCX**
* 📃 Otros archivos de texto compatibles

---

## 🛠️ Tecnologías utilizadas

* 🐍 **Python**
* 🗄️ **SQLite (FTS5 – Full Text Search)**
* 🖼️ **Tkinter** para la interfaz gráfica
* 📄 **PyMuPDF**
* 📝 **python-docx**
* 🧵 **ThreadPoolExecutor** para procesamiento concurrente

---

## ⚙️ Instalación

1️⃣ Clonar el repositorio:

```bash
git clone https://github.com/tuusuario/buscador-juridico-avanzado.git
```

2️⃣ Entrar al directorio:

```bash
cd buscador-juridico-avanzado
```

3️⃣ Ejecutar el programa:

```bash
python buscador.py
```

La primera vez que se ejecute, el sistema **instalará automáticamente las dependencias necesarias**.

---

## 🚀 Uso

1. Selecciona la **carpeta que contiene los documentos jurídicos**.
2. El sistema creará automáticamente un **índice de búsqueda**.
3. Escribe **palabras o frases clave**.
4. El buscador mostrará los **resultados encontrados dentro de los documentos**.

La base de datos de indexación se guarda automáticamente como:

```
Jurisbot_Relatoria.db
```

---

## 👩‍⚖️ Público objetivo

Este proyecto está pensado principalmente para:

* ⚖️ Funcionarios judiciales
* 📚 Auxiliares de relatoría
* 🧑‍💻 Investigadores jurídicos
* 🎓 Estudiantes de derecho
* 📑 Analistas jurídicos

---

## 🧪 Estado del proyecto

🚧 Proyecto en desarrollo y mejora continua.

---

## 📜 Licencia

Uso académico y experimental.
