queries_rdb = {
	'read': {
        'actor': '''
            SELECT *
            FROM actor
            ;
        ''',
        'film': '''
            SELECT *
            FROM film
            ;
        ''',
        'film_actor': '''
            SELECT *
            FROM film_actor
            ;
        ''',
        'actor_yyyymm': '''
            SELECT {} AS yyyymm, *
            FROM actor
            ;
        ''',
        'film_yyyymm': '''
            SELECT {} AS yyyymm, *
            FROM film
            ;
        ''',
        'film_actor_yyyymm': '''
            SELECT {} AS yyyymm, *
            FROM film_actor
            ;
        '''
        },
    'create': {
        'actor_back_v1': '''
            INSERT INTO actor_back_v1 VALUES (%s, %s, %s, %s)
        ''',
        'film_back_v1': '''
            INSERT INTO film_back_v1 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ''',
        'actor_back_v2': '''
            INSERT INTO actor_back_v2 VALUES (%s, %s, %s, %s, %s)
            ;
        ''',
        'film_back_v2': '''
            INSERT INTO film_back_v2 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ;
        '''
		}
	}