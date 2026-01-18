#!/usr/bin/env python3
"""
Script de visualizacion de datos del pipeline ETL.
Genera graficos basados en los datos procesados.
"""
import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Agregar src al path
sys.path.append(str(Path(__file__).parent.parent))

def load_data(output_dir: str = 'output'):
    """Carga los datos desde los archivos Parquet."""
    output_path = Path(output_dir)
    curated_dir = output_path / 'curated'
    
    dim_user = pd.read_parquet(curated_dir / 'dim_user.parquet')
    dim_product = pd.read_parquet(curated_dir / 'dim_product.parquet')
    fact_order = pd.read_parquet(curated_dir / 'fact_order.parquet')
    
    return dim_user, dim_product, fact_order

def plot_sales_by_day(fact_order: pd.DataFrame, output_file: str = None):
    """Grafico: Ventas totales por dia."""
    sales_by_day = fact_order.groupby('order_date').agg({
        'order_id': 'nunique',
        'line_total': 'sum',
        'amount': 'mean'
    }).reset_index()
    
    sales_by_day.columns = ['order_date', 'total_orders', 'total_revenue', 'avg_order_value']
    sales_by_day = sales_by_day.sort_values('order_date')
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    
    # Grafico 1: Revenue por dia
    ax1.plot(sales_by_day['order_date'], sales_by_day['total_revenue'], 
             marker='o', linewidth=2, markersize=8)
    ax1.set_title('Ventas Totales por Dia', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Fecha')
    ax1.set_ylabel('Revenue Total ($)')
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Grafico 2: Numero de ordenes por dia
    ax2.bar(sales_by_day['order_date'], sales_by_day['total_orders'], 
            color='steelblue', alpha=0.7)
    ax2.set_title('Numero de Ordenes por Dia', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Fecha')
    ax2.set_ylabel('Total Ordenes')
    ax2.grid(True, alpha=0.3, axis='y')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Grafico guardado: {output_file}")
    else:
        plt.show()
    
    plt.close()

def plot_top_products_by_category(fact_order: pd.DataFrame, dim_product: pd.DataFrame, 
                                  output_file: str = None, top_n: int = 10):
    """Grafico: Top productos por categoria."""
    merged = fact_order.merge(dim_product, on='sku', how='left')
    
    top_products = merged.groupby(['category', 'sku', 'name']).agg({
        'qty': 'sum',
        'line_total': 'sum'
    }).reset_index()
    
    top_products = top_products.sort_values('line_total', ascending=False).head(top_n)
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    colors = plt.cm.Set3(range(len(top_products)))
    bars = ax.barh(range(len(top_products)), top_products['line_total'], color=colors)
    
    ax.set_yticks(range(len(top_products)))
    ax.set_yticklabels([f"{row['name']} ({row['category']})" 
                        for _, row in top_products.iterrows()])
    ax.set_xlabel('Revenue Total ($)', fontsize=12)
    ax.set_title(f'Top {top_n} Productos por Revenue', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3, axis='x')
    
    # Agregar valores en las barras
    for i, (idx, row) in enumerate(top_products.iterrows()):
        ax.text(row['line_total'], i, f"${row['line_total']:,.2f}", 
                va='center', ha='left', fontsize=9)
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Grafico guardado: {output_file}")
    else:
        plt.show()
    
    plt.close()

def plot_users_by_country(dim_user: pd.DataFrame, fact_order: pd.DataFrame, 
                          output_file: str = None):
    """Grafico: Analisis de usuarios por pais."""
    merged = dim_user.merge(fact_order, on='user_id', how='left')
    
    country_stats = merged.groupby('country').agg({
        'user_id': 'nunique',
        'order_id': 'nunique',
        'line_total': 'sum',
        'amount': 'mean'
    }).reset_index()
    
    country_stats.columns = ['country', 'total_users', 'total_orders', 
                             'total_revenue', 'avg_order_value']
    country_stats = country_stats.sort_values('total_revenue', ascending=False)
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Grafico 1: Revenue por pais
    ax1.bar(country_stats['country'], country_stats['total_revenue'], 
            color='steelblue', alpha=0.7)
    ax1.set_title('Revenue Total por Pais', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Pais')
    ax1.set_ylabel('Revenue ($)')
    ax1.grid(True, alpha=0.3, axis='y')
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Grafico 2: Usuarios por pais (pie chart)
    ax2.pie(country_stats['total_users'], labels=country_stats['country'], 
            autopct='%1.1f%%', startangle=90)
    ax2.set_title('Distribucion de Usuarios por Pais', fontsize=12, fontweight='bold')
    
    # Grafico 3: Ordenes por pais
    ax3.bar(country_stats['country'], country_stats['total_orders'], 
            color='coral', alpha=0.7)
    ax3.set_title('Numero de Ordenes por Pais', fontsize=12, fontweight='bold')
    ax3.set_xlabel('Pais')
    ax3.set_ylabel('Total Ordenes')
    ax3.grid(True, alpha=0.3, axis='y')
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Grafico 4: Valor promedio de orden por pais
    ax4.bar(country_stats['country'], country_stats['avg_order_value'], 
            color='green', alpha=0.7)
    ax4.set_title('Valor Promedio de Orden por Pais', fontsize=12, fontweight='bold')
    ax4.set_xlabel('Pais')
    ax4.set_ylabel('Avg Order Value ($)')
    ax4.grid(True, alpha=0.3, axis='y')
    plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Grafico guardado: {output_file}")
    else:
        plt.show()
    
    plt.close()

def plot_order_distribution(fact_order: pd.DataFrame, output_file: str = None):
    """Grafico: Distribucion de ordenes."""
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(16, 12))
    
    # Grafico 1: Distribucion de cantidad de items por orden
    items_per_order = fact_order.groupby('order_id')['qty'].sum()
    ax1.hist(items_per_order, bins=20, color='steelblue', alpha=0.7, edgecolor='black')
    ax1.set_title('Distribucion de Items por Orden', fontsize=12, fontweight='bold')
    ax1.set_xlabel('Cantidad de Items')
    ax1.set_ylabel('Frecuencia')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Grafico 2: Distribucion de valores de orden
    order_values = fact_order.groupby('order_id')['amount'].first()
    ax2.hist(order_values, bins=20, color='coral', alpha=0.7, edgecolor='black')
    ax2.set_title('Distribucion de Valores de Orden', fontsize=12, fontweight='bold')
    ax2.set_xlabel('Valor de Orden ($)')
    ax2.set_ylabel('Frecuencia')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Grafico 3: Ordenes por mes
    fact_order['order_month'] = pd.to_datetime(fact_order['order_date']).dt.to_period('M')
    orders_by_month = fact_order.groupby('order_month')['order_id'].nunique()
    ax3.bar(range(len(orders_by_month)), orders_by_month.values, 
            color='green', alpha=0.7)
    ax3.set_title('Ordenes por Mes', fontsize=12, fontweight='bold')
    ax3.set_xlabel('Mes')
    ax3.set_ylabel('Numero de Ordenes')
    ax3.set_xticks(range(len(orders_by_month)))
    ax3.set_xticklabels([str(period) for period in orders_by_month.index], rotation=45, ha='right')
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Grafico 4: Top 10 SKUs mas vendidos
    top_skus = fact_order.groupby('sku')['qty'].sum().sort_values(ascending=False).head(10)
    ax4.barh(range(len(top_skus)), top_skus.values, color='purple', alpha=0.7)
    ax4.set_yticks(range(len(top_skus)))
    ax4.set_yticklabels(top_skus.index)
    ax4.set_title('Top 10 SKUs Mas Vendidos', fontsize=12, fontweight='bold')
    ax4.set_xlabel('Cantidad Total Vendida')
    ax4.grid(True, alpha=0.3, axis='x')
    
    plt.tight_layout()
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Grafico guardado: {output_file}")
    else:
        plt.show()
    
    plt.close()

def main():
    """Funcion principal."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Genera graficos de visualizacion de datos ETL')
    parser.add_argument('--output-dir', type=str, default='output',
                        help='Directorio con datos procesados')
    parser.add_argument('--save-dir', type=str, default='output/visualizations',
                        help='Directorio para guardar graficos')
    parser.add_argument('--show', action='store_true',
                        help='Mostrar graficos en pantalla')
    
    args = parser.parse_args()
    
    print("Cargando datos...")
    try:
        dim_user, dim_product, fact_order = load_data(args.output_dir)
        print(f"Cargados: {len(dim_user)} usuarios, {len(dim_product)} productos, {len(fact_order)} items de orden")
    except Exception as e:
        print(f"Error cargando datos: {e}")
        sys.exit(1)
    
    save_dir = Path(args.save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)
    
    print("\nGenerando graficos...")
    
    # Generar todos los graficos
    plot_sales_by_day(
        fact_order, 
        output_file=str(save_dir / 'sales_by_day.png') if not args.show else None
    )
    
    plot_top_products_by_category(
        fact_order, dim_product,
        output_file=str(save_dir / 'top_products.png') if not args.show else None
    )
    
    plot_users_by_country(
        dim_user, fact_order,
        output_file=str(save_dir / 'users_by_country.png') if not args.show else None
    )
    
    plot_order_distribution(
        fact_order,
        output_file=str(save_dir / 'order_distribution.png') if not args.show else None
    )
    
    print("\nVisualizaciones completadas!")

if __name__ == '__main__':
    main()

