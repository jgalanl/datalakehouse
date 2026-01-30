#!/bin/bash
# ============================================================================
# Data Lakehouse Platform - Cleanup Script
# ============================================================================
# This script stops and removes all Docker containers and volumes

set -e

echo "=========================================="
echo "Data Lakehouse Platform Cleanup"
echo "=========================================="
echo ""

read -p "Are you sure you want to stop and remove all containers? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Stopping Docker Compose services..."
    docker-compose down -v
    
    echo "✓ All services stopped and volumes removed"
else
    echo "Cleanup cancelled"
fi

echo ""
