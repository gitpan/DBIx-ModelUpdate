use Test::More tests => 6;

use DBI;

use DBIx::ModelUpdate;
use Storable ('dclone');

$| = 1;

SKIP: {

	eval { require DBD::mysql };
	
	skip "DBD::mysql not installed", 6 if $@;
	
	my $db = DBI -> connect ('DBI:mysql:test', 'test', '', {RaiseError => 1});
	
	ok ($db && $db -> ping (), 'Connected');
	$db -> {RaiseError} = 1;
	
	my $update = DBIx::ModelUpdate -> new (
		$db, 
		dump_to_stderr => 0, 
		before_assert => sub {
		
			my ($self, %params) = @_;
			
			my $needed_tables = $params {tables};
			
		
			while (my ($name, $definition) = each %$needed_tables) {

				next if $name =~ /^__log_/;

				my $log_def = dclone ($definition);

				delete $log_def -> {columns} -> {id} -> {_EXTRA};
				delete $log_def -> {columns} -> {id} -> {_PK};
				$log_def -> {columns} -> {id} -> {TYPE_NAME} ||= 'int';

				delete $log_def -> {data};

				$log_def -> {columns} -> {__dt} = {
					TYPE_NAME => 'datetime',
				};

				$log_def -> {columns} -> {__id} = {
					TYPE_NAME  => 'int', 
					_EXTRA => 'auto_increment', 
					_PK    => 1,
				};

				$log_def -> {columns} -> {__op} = {
					TYPE_NAME  => 'int', 
				};

				$log_def -> {columns} -> {__id_log} = {
					TYPE_NAME  => 'int', 
				};

				$params {tables} -> {'__log_' . $name} = $log_def;			

			}
			
		}
		
	);
	
	
	
	
	
	
	
	ok ($update, 'Object created');
	
	$db -> do ('DROP TABLE IF EXISTS _db_model_checksums');
	$db -> do ('DROP TABLE IF EXISTS users');
	$db -> do ('DROP TABLE IF EXISTS sex');
	$db -> do ('DROP TABLE IF EXISTS __log_users');
	$db -> do ('DROP TABLE IF EXISTS __log_sex');
	
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
				COLUMN_DEF   => 'New user',
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
				_EXTRA => 'auto_increment',
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
	$db -> do ('DROP TABLE __log_users');
	$db -> do ('DROP TABLE __log_sex');
	$db -> do ('DROP TABLE _db_model_checksums');

	$db -> disconnect;
		
}
