import sqlite3
from typing import List, Dict, Any, Optional
import pandas as pd

class BusinessDataQuerier:
    def __init__(self, db_path: str):
        """Initialize the database connection."""
        self.db_path = db_path
        self.conn = None
        self._connect()
    
    def _connect(self):
        """Establish database connection."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print("Connected to database successfully!")
            self.conn.row_factory = sqlite3.Row  # Enable column access by name
        except sqlite3.Error as e:
            raise Exception(f"Database connection failed: {e}")
    
    def search_by_business(self, search_term: str, columns: List[str] = None) -> List[Dict]:
        """
        Search businesses and return them with their affiliates.
        
        Args:
            search_term: Term to search for in business data
            columns: Specific business columns to search in (if None, searches common text fields)
        """
        if columns is None:
            # Default searchable columns - adjust based on your schema
            columns = ['organization_id', 'rcNumber','approvedName', 'natureOfBusinessFk', 'classificationFk', 'address']
        
        # Build dynamic WHERE clause
        where_conditions = []
        params = []
        
        for col in columns:
            where_conditions.append(f"b.{col} LIKE ?")
            params.append(f"%{search_term}%")
        
        where_clause = " OR ".join(where_conditions)
        
        query = f"""
        SELECT 
            b.id AS business_id,
            b.approvedName AS business_name,
            b.rcNumber AS business_number,
            b.address,
            a.id AS affiliate_id,
            a.surname,
            a.firstname AS affiliate_firstname,
            a.otherName AS affiliate_othername,
            a.gender AS affiliate_gender,
            a.email AS affiliate_email,
            a.phoneNumber AS affiliate_phone,
            a.organization_id AS affiliate_organization_id,
            a.city AS affiliate_city,
            a.occupation AS affiliate_occupation
        FROM organizations_old b
        LEFT JOIN affiliates a ON b.organization_id = a.organization_id
        WHERE {where_clause}
        ORDER BY b.id, a.id
        """
        
        return self._execute_search(query, params)
    
    def search_by_affiliate(self, search_term: str, columns: List[str] = None) -> List[Dict]:
        """
        Search affiliates and return associated businesses with all affiliates.
        
        Args:
            search_term: Term to search for in affiliate data
            columns: Specific affiliate columns to search in
        """
        if columns is None:
            columns = ['affiliate_name', 'affiliate_type', 'contact_info']
        
        # Build dynamic WHERE clause
        where_conditions = []
        params = []
        
        for col in columns:
            where_conditions.append(f"a.{col} LIKE ?")
            params.append(f"%{search_term}%")
        
        where_clause = " OR ".join(where_conditions)
        
        query = f"""
        SELECT DISTINCT
            b.*,
            a2.id AS affiliate_id,
            a2.surname,
            a2.firstname AS affiliate_name,
            a2.email AS affiliate_email
        FROM organizations b
        INNER JOIN affiliates a ON b.organization_id = a.organization_id
        LEFT JOIN affiliates a2 ON b.organization_id = a2.organization_id
        WHERE {where_clause}
        ORDER BY b.id, a2.id
        """
        
        return self._execute_search(query, params)
    
    def search_combined(self, search_term: str, business_columns: List[str] = None, 
                       affiliate_columns: List[str] = None) -> List[Dict]:
        """
        Search across both business and affiliate data.
        """
        business_results = self.search_by_business(search_term, business_columns)
        affiliate_results = self.search_by_affiliate(search_term, affiliate_columns)
        
        # Combine and deduplicate results
        combined = {}
        
        for result in business_results + affiliate_results:
            key = (result['business_id'], result.get('affiliate_id'))
            if key not in combined:
                combined[key] = result
        
        return list(combined.values())
    
    def _execute_search(self, query: str, params: List[str]) -> List[Dict]:
        """Execute search query and return results."""
        try:
            cursor = self.conn.execute(query, params)
            results = cursor.fetchall()
            return [dict(row) for row in results]
        except sqlite3.Error as e:
            raise Exception(f"Query execution failed: {e}")
    
    def get_business_with_affiliates(self, business_id: int) -> Dict[str, Any]:
        """Get complete business profile with all affiliates."""
        query = """
        SELECT 
            b.*,
            a.affiliate_id,
            a.affiliate_name,
            a.affiliate_type,
            a.contact_info
        FROM business b
        LEFT JOIN affiliates a ON b.business_id = a.business_id
        WHERE b.business_id = ?
        ORDER BY a.affiliate_id
        """
        
        results = self._execute_search(query, [business_id])
        
        if not results:
            return None
        
        # Structure the response
        business_data = {k: v for k, v in results[0].items() 
                        if not k.startswith('affiliate_')}
        
        affiliates = []
        for row in results:
            if row['affiliate_id']:
                affiliate = {k: v for k, v in row.items() 
                           if k.startswith('affiliate_')}
                affiliates.append(affiliate)
        
        return {
            'business': business_data,
            'affiliates': affiliates
        }
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()

# Example usage
def main():
    # Initialize the querier
    # querier = BusinessDataQuerier('datasrc/cac-data-contd-32.db')
    querier = BusinessDataQuerier('cac-combined.db')

    
    try:
        # Search examples
        print("=== Search by Business Name ===")
        results = querier.search_by_business("tech", ['business_name'])
        for result in results[:5]:  # Show first 5 results
            print(f"Business: {result.get('business_name')} | "
                  f"Affiliate: {result.get('affiliate_name', 'None')}")
        
        print("\n=== Search by Affiliate Type ===")
        results = querier.search_by_affiliate("supplier", ['affiliate_type'])
        for result in results[:5]:
            print(f"Business: {result.get('business_name')} | "
                  f"Affiliate: {result.get('affiliate_name')} ({result.get('affiliate_type')})")
        
        print("\n=== Combined Search ===")
        results = querier.search_combined("consulting")
        print(f"Found {len(results)} total matches")
        
        print("\n=== Get Complete Business Profile ===")
        business_profile = querier.get_business_with_affiliates(1)
        if business_profile:
            print(f"Business: {business_profile['business']}")
            print(f"Number of affiliates: {len(business_profile['affiliates'])}")
    
    finally:
        querier.close()

if __name__ == "__main__":
    # test_querier = BusinessDataQuerier('datasrc/cac-data-contd-32.db')
    test_querier = BusinessDataQuerier('cac-combined.db')
    # test_querier
    print("=== Search by Business Name ===")
    results = test_querier.search_by_business("tech")
    # results = test_querier.search_by_business("Agridev")

    for result in results[:]:  # Show first 5 results
        # print(result.keys())
        print(f"Business: {result.get('business_name')}  {result.get('business_id')}| "
                f"Affiliate: {result.get('surname', 'None')} {result.get('affiliate_firstname', '-')} {result.get('affiliate_othername', '-')}")
        
  
    
    # main()