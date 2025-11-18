#!/bin/bash

# Setup script for Migration Agent
# This script sets up both DBT and QA Suite applications

set -e  # Exit on error

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo "=================================="
echo "Migration Agent Setup Script"
echo "=================================="
echo ""

# Check for .env file
if [ ! -f ".env" ]; then
    echo "⚠️  .env file not found!"
    if [ -f ".env.example" ]; then
        echo "📝 Creating .env from .env.example..."
        cp .env.example .env
        echo "✅ Created .env file"
        echo "⚠️  Please edit .env with your actual Redshift credentials before proceeding."
        echo ""
        read -p "Press Enter after you've updated .env file..."
    else
        echo "❌ .env.example not found. Please create a .env file manually."
        exit 1
    fi
else
    echo "✅ .env file found"
fi

echo ""
echo "=================================="
echo "Setting up DBT"
echo "=================================="
echo ""

cd "$PROJECT_ROOT/DBT"

# Create dbt_project.yml from example if it doesn't exist
if [ ! -f "dbt_project.yml" ]; then
    if [ -f "dbt_project.yml.example" ]; then
        echo "📝 Creating dbt_project.yml from template..."
        cp dbt_project.yml.example dbt_project.yml
        echo "✅ Created dbt_project.yml"
        echo "⚠️  Please edit dbt_project.yml with your project-specific configuration."
        echo ""
    else
        echo "⚠️  dbt_project.yml.example not found. You'll need to create dbt_project.yml manually."
    fi
else
    echo "✅ dbt_project.yml already exists"
fi

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.6 or higher."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Create virtual environment if it doesn't exist
if [ ! -d "redshift_env" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv redshift_env
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment and install dependencies
echo "📦 Installing Python dependencies..."
source redshift_env/bin/activate

pip install --upgrade pip > /dev/null 2>&1
pip install -r requirements.txt

echo "✅ Python dependencies installed"

# Install dbt packages
echo "📦 Installing dbt packages..."
dbt deps

echo "✅ dbt packages installed"

# Test connection
echo ""
echo "🔌 Testing Redshift connection..."
set -a && source "$PROJECT_ROOT/.env" && set +a
python tests/test_redshift_connection.py

echo ""
echo "=================================="
echo "Setting up QA Suite"
echo "=================================="
echo ""

cd "$PROJECT_ROOT/QA Suite"

# Check if Node.js is installed
if ! command -v node &> /dev/null; then
    echo "❌ Node.js is not installed. Please install Node.js 14 or higher."
    exit 1
fi

echo "✅ Node.js found: $(node --version)"
echo "✅ npm found: $(npm --version)"

# Install npm dependencies
echo "📦 Installing npm dependencies..."
npm install

echo "✅ npm dependencies installed"

# Build TypeScript
echo "🔨 Building TypeScript..."
npm run build

echo "✅ TypeScript compiled"

echo ""
echo "=================================="
echo "✅ Setup Complete!"
echo "=================================="
echo ""
echo "Next steps:"
echo ""
echo "1. To run DBT transformations:"
echo "   cd DBT"
echo "   source redshift_env/bin/activate"
echo "   set -a && source ../.env && set +a"
echo "   dbt run"
echo ""
echo "2. To run QA tests:"
echo "   cd 'QA Suite'"
echo "   npm run qa"
echo ""
echo "See README.md for more details."
echo ""

