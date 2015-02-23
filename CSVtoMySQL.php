<?php

/**
 * PHP CVS to MySQL - 1.0.0
 * Copyright (C) 2015 Charles R Hays http://www.charleshays.com
 *
 * file: CSVtoMySQL.php
 *
 * @version 1.1.0 (2/23/2015)
 *		1.0.0 - release
 *
 * @example1 (static)
 * 		CSVtoMySQL::ToHTML('test.csv');
 *
 * @example2 (object)
 * 		$c2m = new CSVtoMySQL('test.csv');
 * 		$c2m->add_blank_tag('NA');
 * 		$c2m->add_blank_tag('M','PHONE');
 * 		$c2m->set_mysql_file('mymysql.sql');
 * 		$c2m->detect_primary_key();
 * 		$c2m->to_file();
 *

    This library is free software; you can redistribute it and/or
    modify it under the terms of the GNU Lesser General Public
    License as published by the Free Software Foundation; either
    version 2.1 of the License, or (at your option) any later version.

    This library is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
    Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public
    License along with this library; if not, write to the Free Software
    Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */

class CSVtoMySQL
	{
	public $csv_file = ""; // the csv file to read from
	public $mysql_file = ""; // the mysql file to output to
	public $delimiter = ','; // record delimiter

	public $mysql_table_name = '';

	// if $fit_data_sizes set to true then data sizes for numbers and varchar will be constrained to the max given data value in the csv file,
	// otherwise if false the value is expanded to the MAX LENGTH given in the regex match file.
	public $fit_data_sizes = false;//true;

	public $max_line_length = 0; // 0 = unlimited for PHP 5.1+
	public $detect_header = false;//true; // automatically try and detect if there is a header (not 100% reliable)
	public $has_header = true; // if true then the csv file has a header which is used to determine field names, if false, the user must define the field names
	public $primary_key_i = 0; // defaults is first column or index 0 of row array
	public $primary_key_first = true; // set this to make the primary key the first entry (in case it is not)
	public $detect_not_null = true;//false; // this switch when true, will classify the mysql entries as NOT NULL that it finds with no null entries.
	public $field_names_lc = true; // a switch to make field names lowercase if true, otherwise they left as is
	public $fields = array(); // array of Class FieldTypes
	public $num_fields = 0; // the length of the fields array

	// The file containing regex matches and domino order.
	// This is a seperate TXT file so you can more easily adjust regex matches or have cuystom file sets for different needs.
	// The row orders are important! If a match fails on a line, then the starting match will be on the next line.
	// If it reaches ERROR (the last entry) then the entire field is thrown out.
	// FIELD 1: The type name as string to apply if matches
	// FIELD 2: The max length to test regex against
	// FIELD 3: The regex that must match in whole ex: ^.$ (if left blank no regex is applied)
	public $regex_match_file = 'regex_mysql_data.txt';
	public $regex_bounds = '#_#'; // you can modify this if it inteferes with the data being parsed
	public $regex_switches = 'i'; // the regex switches
	public $regex_match_array = array();

	public $regex_deliminators = array("\x02",'#','/','~','|'); // a list of regex ends to try and use. Each is tested against the contents in order until none is found in the value then uses that.

	public $blank_tags = array(''); // a list of data values that when encounted should be considered as blank. ex: -, NA, N/A, null (you must define these with the "add_blank_tag($v)" method.
	public $blank_tags_col = array(); // similar to above this uses "add_blank_tag($v,$col)" to assign apply a blank data value to a specific column only.

	private $user_supplied_primary_key = false;
	public $user_primary_key_inc = 0; // when add_primary_key($name,$type[,$start]) is used the values will be incrementing starting at this value

	private $has_ran_detect_types = false;
	
	public $reserved_words_file = 'reserved_mysql_words.txt'; // A file with a list of words the headers can not be transfered into mysql entries.
	public $rserverd_words_arr = array();

	// constructor
	public function __construct($csv,$mysql = "mysql.sql", $hashead = true)
		{
		$fn = explode('.',basename($csv),2);
		$this->set_table_name($fn[0]);
		$this->set_csv_file($csv);
		$this->set_mysql_file($mysql);
		$this->load_regex();
		$this->load_reserved_words();
		}

	// this method loads the regex file and assigns it to an array $regex_match_array which is an array of class DetectType
	public function load_regex($regex_file = '')
		{
		if($regex_file != '')
			{
			$this->regex_match_file = $regex_file;
			}
		$this->regex_match_array = array();
		$regtmp = file($this->regex_match_file, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
		foreach($regtmp as $k => $v)
			{
			if($v[0] != '#')
				{
				if(strpos($v,',') > 1)
					{
					$f = explode(',',$v,3);
					$this->regex_match_array[] = new CSVtoMySQL_DetectType($f[0],$f[1],$f[2]);
					}
				}
			}
		}

	public function load_reserved_words($f = '')
		{
		if($f == '')
			{
			$f = $this->reserved_words_file;
			}
		$arr = file($f, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
		foreach($arr as $k => $v)
			{
			$this->reserved_words_arr[] = trim(strtolower($v));
			}
			//print_r($this->reserved_words_arr);
		}

	// set the csv file to load
	public function set_csv_file($file)
		{
		$this->csv_file = $file;
		}

	// set the mysql file to output to
	public function set_mysql_file($file)
		{
		$this->mysql_file = $file;
		}

	// set the csv record splitting delimiter
	public function set_delimiter($v)
		{
		$this->delimiter = $v;
		}
		
	public function set_table_name($s)
		{
		$this->mysql_table_name = preg_replace("/[^A-Za-z0-9]/", "", $s);
		}

	// set the max line length of the csv file, 0 = unlimited in PHP 5.1+
	public function set_max_line_length($v)
		{
		$this->max_line_length = $v;
		}

	// add a new field, note that this will append field to the end of any current field list
	public function add_field($v,$type = 'VARCHAR(255)')
		{
		$this->fields[] = new CSVtoMySQL_FieldType($this,$v,$type);
		$this->num_fields++;
		}

	// change the field name based on $n which can be an index number or name
	public function change_field_name($n,$name)
		{
		if($this->has_ran_detect_types == false) $this->detect_types(); // if this method has not been ran it needs to be done before proceeding.

		if(($n > 0) || ($n == '0')) // by index
			{
			if(isset($this->fields[$n]) == true)
				{
				$this->fields[$n]->field = $name;
				return true;
				}
			return false;
			}
		foreach($this->fields as $k => $v)
			{
			if($v->original_name == $n) // can match by original column name
				{
				$v->field = $name;
				return true;
				}
			else if($this->field == $n) // or if modified the new column name
				{
				$v->field = $k;
				return true;
				}
			}
		return false;
		}

	// set the primary key index. If this is a number it sets the key as the field index 
	// if it is a string it must match one of the field column names.
	public function primary_key($v)
		{
		if(($v > 0) || ($v == '0'))
			{
			return $this->primary_key_col_by_number($v);
			}
		return $this->primary_key_col_by_name($v);
		}

	public function primary_key_col_by_name($s)
		{
		foreach($this->fields as $k => $v)
			{
			if($v->original_name == $s) // can match by original column name
				{
				$this->primary_key_i = $k;
				return true;
				}
			else if($this->field == $s) // or if modified the new column name
				{
				$this->primary_key_i = $k;
				return true;
				}
			}
		return false;
		}

	// where first column of the index = 0, not 1!!!
	public function primary_key_col_by_number($n)
		{
		if($n >= $this->num_fields) return false; // can not assign primary key index to an index larger then the number of fields
		$this->primary_key_i = $n;
		return true;
		}

	public function add_primary_key($name = 'id',$type = 'INT',$start_at = -1)
		{
		if($this->has_ran_detect_types == false) $this->detect_types(); // if this method has not been ran it needs to be done before proceeding.
		$this->user_supplied_primary_key = true;
		$pk = new CSVtoMySQL_FieldType($this,$name,$type);
		if($start_at > -1)
			{
			$this->user_primary_key_inc = $start_at;
			}
		$this->fields[] = $pk;
		$this->num_fields++;
		$this->primary_key_col_by_name($name);
		$this->primary_key_adjust();
		}

	// This method will scan the csv record to try and determine which column is best suited for the primary key
	// starting from column index 0 to the last column, if you supply $n as a number it start with that index
	// if supplied by a name it starts at that column header name.
	// NOTE! It is best to supply the primary key using primary_key($v) as using this detect method can be very
	// slow and memory exhausting.
	public function detect_primary_key($n = '')
		{
		if($this->has_ran_detect_types == false) $this->detect_types(); // if this method has not been ran it needs to be done before proceeding.
		//if($this->user_supplied_primary_key == true) return true; // we already have a user defined primary key

		$index = 0;
		if($n != '') // $n was supplied
			{
			if($n > 0) // was  anumber
				{
				$index = $n;
				}
			else // must be a string that matches the header column name
				{
				foreach($this->fields as $k => $v)
					{
					if($v->original_name == $n)
						{
						$index = $k;
						break;
						}
					else if($v->field == $n)
						{
						$index = $k;
						break;
						}
					}
				}
			}

		// start parsing to detect primary key
		for($i=$index; $i < $this->num_fields; $i++)
			{
			if($this->fields[$i]->can_be_primary_key() == false) continue; // this type is not eligible to be a primary key so skip
			$tmp_arr = array();
			$duplicate = false;
			if(($handle = fopen($this->csv_file, "r")) !== false)
				{
				$f = 0;
				if($this->has_header == false) $f = 1;
				while(($data = fgetcsv($handle, $this->max_line_length, $this->delimiter)) !== false)
					{
					if($f == 1)
						{
						if($data[$i] == '') // a primary key can not have null entries
							{
							$duplicate = true;
							break;
							}
						if(in_array($data[$i], $tmp_arr, true)) // found a duplicate so throw out row
							{
							$duplicate = true;
							break; // exit the while and returns to the for loop
							}
						$tmp_arr[] = $data[$i]; // add this data tot he tmp array
						}
					else
						{
						$f = 1;
						}
					$data = null; // wipe out varible
					} // end while
				fclose($handle);
				
				// At the end of the column pass check if the duplicate var had been tripped
				if($duplicate == false) // if not this should be our primary key
					{
					$this->primary_key_col_by_number($i);
					$this->primary_key_adjust();
					return true; // primary key found, exit and return true
					}
				}
			}
		return false; // pass exhausted, no primary key so return false.
		}

	public function array_has_duplicates($arr)
		{
		$dupe_array = array();
		foreach($arr as $v)
			{
			if(++$dupe_array[$v] > 1)
				{
				return true;
				}
			}
		return false;
		}

	// when $primary_key_first is true and the key index is not the first, this method
	// readjusts the array to put the primary key at the beginning.
	public function primary_key_adjust()
		{
		if($this->primary_key_first == true)
			{
			if($this->primary_key_i > 0)
				{
				$new_fields = array();
				$new_fields[] = $this->fields[$this->primary_key_i];
				foreach($this->fields as $k => $v)
					{
					if($k != $this->primary_key_i)
						{
						$new_fields[] = $v;
						}
					}

				$this->fields = $new_fields;
				$this->primary_key_i = 0; // more the primary key index to front
				}
			}
		}

	// outputs a string of the types assigned. (usefull for testing)
	public function print_types()
		{
		if($this->has_ran_detect_types == false) $this->detect_types();

		foreach($this->fields as $k => $v)
			{
			if($this->primary_key_i == $k)
				{
				echo '[PRIMARY KEY] ';
				if($this->fields[$k]->can_auto_increment() == true) echo '[AUTO_INCREMENT] ';
				}
			echo $v->original_name.' => '.$v->field.' => '.$v->type."\n";
			}
		}

	// outputs a string of the types assigned like above but with html br breaks for html display
	public function print_html_types()
		{
		if($this->has_ran_detect_types == false) $this->detect_types();

		foreach($this->fields as $k => $v)
			{
			if($this->primary_key_i == $k)
				{
				echo '[PRIMARY KEY] ';
				if($this->fields[$k]->can_auto_increment() == true) echo '[AUTO_INCREMENT] ';
				}
			echo $v->original_name.' => '.$v->field.' => '.$v->type."</br>\n";
			}
		}
		
		
	//
	// send output to string
	//
	private $this_to_string = '';
	public function to_string()
		{
		$this->this_to_string = '';
		$this->build_output('_to_string');
		return $this->this_to_string;
		}
	private function _to_string($v,$nl = false) // callback helper
		{
		$this->this_to_string .= $v;
		if($nl == true) $this->this_to_string .= "\n"; // newline
		}

	//
	// send output to screen
	//
	public function to_screen()
		{
		$this->build_output('_to_screen');
		}
	private function _to_screen($v,$nl = false) // callback helper
		{
		echo $v;
		if($nl == true) echo "\n"; // newline
		}

	//
	// send output to HTML
	//
	public function to_html()
		{
		$this->build_output('_to_html');
		}
	private function _to_html($v,$nl = false) // callback helper
		{
		echo $v;
		if($nl == true) echo "</br>\n"; // newline
		}

	//
	// send output to file
	//
	public function to_file($file = '')
		{
		if($file != '') // user supplied a file name
			{
			$this->set_mysql_file($file);
			}
		$r = file_put_contents($this->mysql_file,''); // create file and zero out
		if($r ===  false) return false;
		$this->build_output('_to_file');
		return true;
		}
	private function _to_file($v,$nl = false) // callback helper
		{
		if($nl == true) $v .= "\n";
		file_put_contents($this->mysql_file, $v, FILE_APPEND | LOCK_EX);
		}
		
	//
	// send to mysql
	//
	// NOTE: Before using this method you need to already have connected to the mysql and selected database. (see following lines as example)
	// $sql = mysql_connect('xxx.xxx.xxx.xxx', 'user', 'password');
	// mysql_select_db('database',$sql);
	public function to_mysql()
		{
		$r = @mysql_ping() ? true : false;
		if($r == false) return false;

		// DROP TABLE
		$this->this_to_string = '';
		$this->build_output_droptable('_to_string');
		$r = mysql_query($this->this_to_string);

		// CREATE TABLE
		$this->this_to_string = '';
		$this->build_output_table('_to_string');
		$r = mysql_query($this->this_to_string);

		// CREATE INSERT LIST
		$this->this_to_string = '';
		$r = $this->build_output_inserts('_to_mysql');

		return true;
		}

	private function _to_mysql($v,$nl = false) // callback helper
		{
		$this->this_to_string .= $v;
		if($nl == true) // new lines indicate insert
			{
			mysql_query($this->this_to_string);
			$this->this_to_string = '';
			}
		}

	// wrapper for the 3 types of methods to be used by to_screen and to_file
	public function build_output($f)
		{
		// DROP TABLE
		$this->build_output_droptable($f);

		// CREATE TABLE
		$this->build_output_table($f);

		// CREATE INSERT LIST
		$this->build_output_inserts($f);
		}

	// method that takes callback to create drop table
	public function build_output_droptable($f)
		{
		if($this->has_ran_detect_types == false) $this->detect_types();

		// DROP TABLE
		$this->$f('DROP TABLE '.$this->mysql_table_name.';',true);
		}

	// method that takes callback to create mysql table
	public function build_output_table($f)
		{
		if($this->has_ran_detect_types == false) $this->detect_types();

		// CREATE TABLE
		$this->$f('CREATE TABLE '.$this->mysql_table_name.'(',true);
		$i = 1;
		foreach($this->fields as $k => $v)
			{
			$this->$f('`'.$v->field.'` '.$v->type);
			if($this->primary_key_i == $k)
				{
				if($this->fields[$k]->can_auto_increment() == true) $this->$f(' AUTO_INCREMENT');
				$this->$f(' PRIMARY KEY');
				}
			if($this->num_fields > $i)
				{
				$this->$f(',');
				}
			$this->$f('',true);
			$i++;
			}

		$this->$f(');',true);
		}
		
	// method that takes callback to create inserts
	public function build_output_inserts($f)
		{
		if($this->has_ran_detect_types == false) $this->detect_types();

		// CREATE INSERT LIST
		if(($handle = fopen($this->csv_file, "r")) !== false)
			{
			$c = 0;
			if($this->has_header == false) $f = 1;
			while(($data = fgetcsv($handle, $this->max_line_length, $this->delimiter)) !== false)
				{
				if($c == 1)
					{
					$this->$f('INSERT INTO `'.$this->mysql_table_name.'` (');

					$s = '';
					foreach($this->fields as $k => $v)
						{
						if($s != '') $s .= ',';
						$s .= $v->field;
						}
					$this->$f($s);

					$this->$f(') VALUES (');

					$s = '';

					// must rebuild $data array to incorporate any user modificatins.
					$data2 = array();
					foreach($this->fields as $k => $v)
						{
						if($v->original_index != -1)
							{
							if($v->type == 'DATE') // reformat date to mysql format
								{
								$data2[] = date('Y-m-d', strtotime(str_replace('-', '/',$data[$v->original_index])));
								}
							else if($v->type == 'DATETIME') // reformat for datetime
								{
								$data2[] = date('Y-m-d H:i:s', strtotime(str_replace('-', '/', $data[$v->original_index])));
								}
							else if($v->type == 'TIME') // reformat for time
								{
								$date2[] = date('H:i:s', strtotime('6:30 PM'));
								}
							else
								{
								$data2[] = $data[$v->original_index];
								}
							}
						else
							{
							$data2[] = ''; // non data indexed is user added fields which we have no data for
							}
						}

					foreach($data2 as $k => $v)
						{
						if($s != '') $s .= ',';

						if($this->primary_key_i == $k)
							{
							if($this->user_supplied_primary_key == true)
								{
								$s .= '\''.$this->user_primary_key_inc.'\'';
								$this->user_primary_key_inc++;
								}
							else
								{
								$s .= '\''.addslashes($v).'\'';
								}
							}
						else
							{
							$s .= '\''.addslashes($v).'\'';
							}
						}
					$this->$f($s);

					$this->$f(');');
					$this->$f('',true); // also signals inserts to post to myqsql
					}
				else
					{
					$c = 1;
					}
				$data = null; // wipe out varible
				} // end while
			fclose($handle);
			}
		}

	// adding a blank tag to the blank_tags array allows us to treat data that was intended to represent blank or not available as actual blank
	// where it would have caused the regex matching to default to something untinded.
	// if $col is passed then it will assign this blank tag only to that specfic colum.
	public function add_blank_tag($v,$col = '')
		{
		if($col == '')
			{
			$this->blank_tags[] = $v;
			}
		else
			{
			if($this->blank_tags_col[base64_encode($col)] == null)
				{
				$this->blank_tags_col[base64_encode($col)] = array();
				}
			$this->blank_tags_col[base64_encode($col)][] = $v;
			}
		}

	// This is the pre parse to determine what field columns get assigned what mysql data type.
	public function detect_types()
		{
		if(($handle = fopen($this->csv_file, "r")) !== false)
			{
			$f = 0;
			if($this->has_header == false) $f = 1;
			while(($data = fgetcsv($handle, $this->max_line_length, $this->delimiter)) !== false)
				{
				if($f == 1)
					{
					// going to look at each entry and determine the best type
					for($i=0; $i < $this->num_fields; $i++)
						{
						if(isset($data[$i]))
							{
							if($data[$i] == '') // this field column can be null
								{
								$this->fields[$i]->can_be_null = true;
								}
							if($this->not_a_blank($data[$i],$this->fields[$i]->original_name))
								{
								$this->fields[$i]->check(trim($data[$i]));
								}
							}
						}
					}
				else
					{
					$f = 1;
					$this->detect_header(implode('#_#',$data)); // detect a header
					if($this->has_header == true) // yes there is a header use it to build fields now
						{
						foreach($data as $k => $v)
							{
							$nf = new CSVtoMySQL_FieldType($this,$v);
							$nf ->original_index = $k;
							$this->fields[] = $nf;
							}
						}
					else // no header so make up the fields "v0,v1,v2,v3"
						{
						foreach($data as $k => $v)
							{
							$nf = new CSVtoMySQL_FieldType($this,'v'.$k);
							$nf ->original_index = $k;
							$this->fields[] = $nf;
							}
						}
					$this->num_fields = count($this->fields);

					}
				$data = null; // wipe out varible
				} // end while
			fclose($handle);
			}
		$this->has_ran_detect_types = true;
		}

	// checks if the data $d value exists as a global blank or a column specific blank and returns false if found
	public function not_a_blank($d,$col)
		{
		if($d == null) return false; // null is definantly blank

		if($this->blank_tags != null)
			{
			if(in_array($d,$this->blank_tags,true)) return false; // found in global blank tags
			}

		$col = base64_encode($col);

		if(isset($this->blank_tags_col[$col]))
			{
			if(in_array($d,$this->blank_tags_col[$col],true)) return false; // found in column specific blank tags
			}

		return true;
		}

	// Used to try and detect if the CVS file contains a header. This is very problematic and not 100% accurate.
	// By default the public variable $detect_header = false and must be set to true for this method to work. Otherwise it assumed there is a header.
	public function detect_header($s)
		{
		$arr = explode('#_#',$s);
		if($this->detect_header == false) return false;

		if(in_array('', $arr, true) == true) // headers should not have blank fields
			{
			$this->has_header = false;
			return false;
			}
		else if($this->array_has_duplicates($arr) == true) // header should not have duplicate fields
			{
			$this->has_header = false;
			return false;
			}
		// we could match if any fields contain data like a number, float or date. But there are instances where these are legit columns but they wouldn't make good mysql data names, so lets do this.
		foreach($arr as $k => $v)
			{
			if(preg_match('/^-?(?:\d+|\d*\.\d+)$/',$v) > 0) // is it a float?
				{
				$this->has_header = false;
				return false;
				}
			// can add more here
			}

		return true; // making it to the end we have detectedt with no real certintity that the first line, possibly maybe could be a header.
		}
		
	/////////////////////////////////////////////////////////////////////////////
	// STATIC METHODS
	/////////////////////////////////////////////////////////////////////////////

	private static $static_obj;
	
	// TO STRING
	// return as string (auto detect primary key)
	public static function ToString($in_file,$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		if(self::$static_obj->detect_primary_key() == false)
			{
			return ToStringMyKey($in_file,$my_key = 'id',$delim = ',');
			}
		return self::$static_obj->to_string();
		}

	// return as string (using an INT id you name in $my_key)
	public static function ToStringMyKey($in_file,$my_key = 'id',$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		self::$static_obj->add_primary_key($my_key,'INT');
		return self::$static_obj->to_string();
		}

	// TO FILE
	// create as a file (auto detect primary key)
	public static function ToFile($in_file,$out_file,$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,$out_file,true);
		self::$static_obj->set_delimiter($delim);
		if(self::$static_obj->detect_primary_key() == false)
			{
			return ToFileMyKey($in_file,$my_key = 'id',$delim = ',');
			}
		return self::$static_obj->to_file();
		}

	// create as a file (using an INT id you name in $my_key)
	public static function ToFileMyKey($in_file,$out_file,$my_key = 'id',$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,$out_file,true);
		self::$static_obj->set_delimiter($delim);
		self::$static_obj->add_primary_key($my_key,'INT');
		return self::$static_obj->to_file();
		}

	// TO SCREEN
	// output to screen (auto detect primary key)
	public static function ToScreen($in_file,$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		if(self::$static_obj->detect_primary_key() == false)
			{
			return ToScreenMyKey($in_file,$my_key = 'id',$delim = ',');
			}
		return self::$static_obj->to_screen();
		}

	// output to screen (using an INT id you name in $my_key)
	public static function ToScreenMyKey($in_file,$my_key = 'id',$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		self::$static_obj->add_primary_key($my_key,'INT');
		return self::$static_obj->to_screen();
		}

	// TO HTML
	// output to HTML (auto detect primary key)
	public static function ToHTML($in_file,$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		if(self::$static_obj->detect_primary_key() == false)
			{
			return ToStringHTML($in_file,$my_key = 'id',$delim = ',');
			}
		return self::$static_obj->to_html();
		}

	// output to HTML (using an INT id you name in $my_key)
	public static function ToHTMLMyKey($in_file,$my_key = 'id',$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		self::$static_obj->add_primary_key($my_key,'INT');
		return self::$static_obj->to_html();
		}

	// TO MYSQL
	// NOTE: For these static method you need to already have a mysql connection and connected to the database
	// add directly to mysql via conneciton (auto detect primary key)
	public static function ToMySQL($in_file,$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		if(self::$static_obj->detect_primary_key() == false)
			{
			return ToMySQLMyKey($in_file,$my_key = 'id',$delim = ',');
			}
		return self::$static_obj->to_mysql();
		}

	// add directly to mysql via conneciton (using an INT id you name in $my_key)
	public static function ToMySQLMyKey($in_file,$my_key = 'id',$delim = ',')
		{
		self::$static_obj = new CSVtoMySQL($in_file,'mysql.sql',true);
		self::$static_obj->set_delimiter($delim);
		self::$static_obj->add_primary_key($my_key,'INT');
		return self::$static_obj->to_mysql();
		}



	} // end class CSVtoMySQL

/////////////////////////////////////////////////////////////////////////////
// CLASS: CSVtoMySQL_DetectType
/////////////////////////////////////////////////////////////////////////////

// This class is used in an array to to contain the the different regex matches loaded from $regex_match_file in the main class CSVtoMySQL.
class CSVtoMySQL_DetectType
	{
	public $type = '';
	public $max_length = 0;
	public $regex = '';
	public $can_be_primary_key = false;
	public $can_auto_increment = false;

	public function __construct($f1,$f2,$f3)
		{
		if($f1[0] == '*') // type starting with * means it can be a primary key
			{
			$this->can_be_primary_key = true;
			$f1 = ltrim($f1,'*');
			if($f1[0] == '+') // if also includes + then it can have auto_increment
				{
				$this->can_auto_increment = true;
				$f1 = ltrim($f1,'+');
				}
			}

		$this->type = $f1;
		$this->max_length = $f2;
		$this->regex = $f3;
		}
	} // end class DetectType

/////////////////////////////////////////////////////////////////////////////
// CLASS: CSVtoMySQL_FieldType
/////////////////////////////////////////////////////////////////////////////

// This class is used in an array of the header columns to process and track what each column type will become.
class CSVtoMySQL_FieldType
	{
	public $original_name = '';
	public $original_index = -1;
	public $field = '';
	public $type = 'NA';//'VARCHAR(250)'; // default
	private $length = 0;
	private $decimals = -1; // space on right side of decimal point
	private $digits = -1; // number of decimals, the full length
	private $dt_index = 0;
	private $locked = false;
	private $error_note = '';
	private $parent_handle;
	private $uniques = true;
	private $decimal_type = false;
	public $can_be_null = false; // null or blank entries found so if if detect_not_null is set this is used to determine

	public function __construct($h,$field_name,$type = '')
		{
		$this->parent_handle = $h; // we will need access back to parent handle

		$this->original_name = trim($field_name);

		$this->field = $this->original_name;

		if($this->parent_handle->field_names_lc == true)
			{
			$this->field = strtolower($this->field);
			}

		// normalize field name
		$this->field = trim($this->field);
		$this->field = trim($this->field,'_');
		$this->field = trim($this->field);
		$this->field = preg_replace('/[^\da-z_]/i', '_',$this->field);
		$this->field = trim($this->field);
		$this->field = trim($this->field,'_');
		$this->field = trim($this->field);
		
		// check if this field name is a reserved word
		if(in_array(strtolower($this->field),$this->parent_handle->reserved_words_arr,true))
			{
			// is a reserved word so we must rename it
			$this->field = '_'.$this->field;//.'1';
			}

		if($type != '') // user defined type
			{
			$this->type = $type;
			$this->lock();
			}
		}

	public function lock()
		{
		$this->locked = true;
		}

	public function unlock()
		{
		$this->locked = false;
		}

	public function can_be_primary_key()
		{
		return $this->parent_handle->regex_match_array[$this->dt_index]->can_be_primary_key; // get this from the DetectType array of current index
		}

	public function can_auto_increment()
		{
		return $this->parent_handle->regex_match_array[$this->dt_index]->can_auto_increment;
		}

	public function check_for_error()
		{
		if($this->parent_handle->regex_match_array[$this->dt_index]->type == 'ERROR') // have reached the ERROR type end
			{
			$this->type = 'ERROR';
			return true;
			}
		if($this->dt_index >= $this->parent_handle->num_fields) // have reached the end of the array index
			{
			$this->type = 'ERROR';
			return true;
			}
		return false;
		}

	public function check($v)
		{
		// when locked we no longer process
		if($this->locked == true) return;

		if($this->check_for_error() == true) return;

		if(strlen($v) > $this->length) $this->length = strlen($v);

		// check based on string length
		while(strlen($v) > $this->parent_handle->regex_match_array[$this->dt_index]->max_length)
			{
			$this->advance_index();
			if($this->check_for_error() == true) return;
			}

		if(trim($this->parent_handle->regex_match_array[$this->dt_index]->regex) != '') // only if a regex exists
			{
			// check based on regex
			while($this->regex_match($v) == 0)
				{
				$this->advance_index();
				if($this->check_for_error() == true) return;

				if(trim($this->parent_handle->regex_match_array[$this->dt_index]->regex) == '') // no regex
					{
					break;
					}
				}
			}
		$this->type = $this->parent_handle->regex_match_array[$this->dt_index]->type; // assign new type

		if(strpos($this->type, '_L_') > 0) // does type have the _L_ tag
			{
			if($this->parent_handle->fit_data_sizes == true) // must fit max size
				{
				$this->type = str_replace('_L_', $this->length, $this->type);
				}
			else // use regex files max length
				{
				$this->type = str_replace('_L_', $this->parent_handle->regex_match_array[$this->dt_index]->max_length, $this->type);
				}
			}

		// before finishing we must also apply the decimal (M,D) values if it is a decimal type
		if($this->decimal_type == true)
			{
			if($this->parent_handle->fit_data_sizes == true) // must fit max size
				{
				$this->type = str_replace('(M_D)','('.$this->digits.','.$this->decimals.')',$this->type);
				}
			else // use regex files max length
				{
				$this->type = str_replace('(M_D)','('.$this->parent_handle->regex_match_array[$this->dt_index]->max_length.','.$this->decimals.')',$this->type);
				}
			}
		}

	public function advance_index()
		{
		$this->dt_index++;

		// Decimal types with (M_D), such as float, double, decimal will need an m,d set.
		// Not using "," comma because that is the deliinator for the regex match types file.
		if(strpos($this->parent_handle->regex_match_array[$this->dt_index]->type,'(M_D)') > 0)
			{
			$this->decimal_type = true;
			$this->decimals = -1; // reset
			$this->digits = -1;
			}
		else
			{
			$this->decimal_type = false;
			$this->decimals = -1; // reset
			$this->digits = -1;
			}
		}

	public function regex_match($v)
		{
		$delim = $this->get_deliminator($this->parent_handle->regex_match_array[$this->dt_index]->regex);

		// decimal rule
		if($this->decimal_type == true)
			{
			// if more than 1 decimal point this is not a decimal type
			if(substr_count($v,'.') > 1) // not a decimal
				{
				return 0;
				}

			// count the m and d
			$x = preg_replace("/[^0-9\.]/", "", $v);
			list($m1,$d1) = explode('.',$v,2);
			$m = strlen($m1);
			$d = strlen($d1);
			if(($m+$d) > $this->digits) $this->digits = ($m+$d); // record highest total digits
			if($d > $this->decimals) $this->decimals = $d; // record highest decimal places
			}

		return preg_match($delim.$this->parent_handle->regex_match_array[$this->dt_index]->regex.$delim.$this->parent_handle->regex_switches,$v);
		}

	public function get_deliminator($regex)
		{
		$c = $this->parent_handle->regex_deliminators[0]; // start with first deliminator

		foreach($this->parent_handle->regex_deliminators as $v)
			{
			if(strpos($regex,$v) === false)
				{
				return $v;
				}
			}
		return $c;
		}

	}// end class FieldType

// EOF
