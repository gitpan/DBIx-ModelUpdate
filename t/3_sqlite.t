use Test::More tests => 6;

use DBI;

use DBIx::ModelUpdate;

$| = 1;

SKIP: {

	eval { require DBD::SQLite };
	
	skip "DBD::SQLite not installed", 6 if $@;
	
	my $db = DBI -> connect ("DBI:SQLite:dbname=/tmp/test.sqlite", '', '', {RaiseError => 1});
	
	ok ($db && $db -> ping (), 'Connected');
	$db -> {RaiseError} = 1;
	
	my $update = DBIx::ModelUpdate -> new ($db, dump_to_stderr => 0);
	ok ($update, 'Object created');
		
	my $users = {
	
		columns => {

			id_sex => {
				TYPE_NAME    => 'int',
				COLUMN_SIZE  => 11,
			},

			salary => {
				TYPE_NAME    => 'decimal',
				COLUMN_SIZE  => 10,
				DECIMAL_DIGITS  => 2,
			},

			name => {
				TYPE_NAME    => 'varchar',
				COLUMN_SIZE  => 50,
#				COLUMN_DEF   => 'New user',
				NULLABLE     => 0,
			},

			password => {
				TYPE_NAME    => 'varchar',
				COLUMN_SIZE  => 255,
			},

		},
		
	};
	
	my $sex_columns = {
	
		name => {
			TYPE_NAME   => 'char',
			COLUMN_SIZE => 6,
		},
	
	};
	
	my $sex = {
	
		columns => $sex_columns,

		data => [

			{id => 10, name => 'male'},
			{id => 21, name => 'emale'},

		]			
	
	};

	my %params = (

		default_columns => {

			id => {
				TYPE_NAME  => 'int',
				_PK    => 1,
				COLUMN_SIZE  => 11,
				NULLABLE  => 0,
			},

		},	

		tables => {
		
			users => $users,
			sex => $sex,
		},
		

	);

	$update -> assert (%params);	
			
	is_deeply ($update -> get_columns ('users'), $users -> {columns}, 'structure');

	$sex -> {columns} -> {name} -> {COLUMN_SIZE} = '255';

	$update -> assert (%params);	

	is_deeply ($update -> get_columns ('users'), $users -> {columns}, 'structure');
	
	my ($name) = $db -> selectrow_array ("SELECT name FROM sex WHERE id = 21");
	
	is ($name, 'emale', 'wrong_sex');
	
	$sex -> {data} -> [1] -> {name} = 'female';

	$update -> assert (%params);	
	
	($name) = $db -> selectrow_array ("SELECT name FROM sex WHERE id = 21");

	is ($name, 'female', 'right_sex');

	$db -> do ('DROP TABLE users');
	$db -> do ('DROP TABLE sex');
	$db -> do ('DROP TABLE _db_model_checksums');

	$db -> disconnect;
		
}
