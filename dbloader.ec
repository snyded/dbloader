/*
    dbloader.ec - generates SQL "load" statements
    Copyright (C) 1990,1994  David A. Snyder
 
    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; version 2 of the License.
 
    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
 
    You should have received a copy of the GNU General Public License
    along with this program; if not, write to the Free Software
    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
*/

#ifndef lint
static char sccsid[] = "@(#) dbloader.ec 1.4  94/09/25 12:08:04";
#endif /* not lint */


#include <ctype.h>
#include <stdio.h>
#include <string.h>
$include sqlca;
$include sqltypes;

#define SUCCESS	0
#define strsch(s1, s2) strnsch(s1, s2, 0)

FILE	*fp;
char	*database = NULL, *table = NULL;
void	exit();

$struct _systables {
	char	tabname[19];
	char	dirpath[65];
	long	tabid;
} systables;

$struct _syscolumns {
	char	colname[19];
} syscolumns;

main(argc, argv)
int	argc;
char	*argv[];
{

	$char	exec_stmt[32], qry_stmt[72];
	extern char	*optarg;
	extern int	optind, opterr;
	int	c, loadflg;
	int	dflg = 0, eflg = 0, errflg = 0, lflg = 0, sflg = 0, tflg = 0;

	/* Determine "load" or "unload" */
	if (strsch(argv[0], "dbloader") != -1)
		loadflg = 1;
	else if (strsch(argv[0], "dbunloader") != -1)
		loadflg = 0;
	else
		exit(1);

	/* Print copyright message */
	if (loadflg)
		(void)fprintf(stderr, "DBLOADER version 1.4, Copyright (C) 1990,1994 David A. Snyder\n\n");
	else
		(void)fprintf(stderr, "DBUNLOADER version 1.4, Copyright (C) 1990,1994 David A. Snyder\n\n");

	/* get command line options */
	while ((c = getopt(argc, argv, "d:elst:")) != EOF)
		switch (c) {
		case 'd':
			dflg++;
			database = optarg;
			break;
		case 'e':
			eflg++;
			break;
		case 'l':
			lflg++;
			if (loadflg)
				errflg++;
			break;
		case 's':
			sflg++;
			if (loadflg)
				errflg++;
			break;
		case 't':
			tflg++;
			table = optarg;
			break;
		default:
			errflg++;
			break;
		}

	/* validate command line options */
	if (errflg || !dflg || (lflg && !sflg)) {
		if (loadflg)
			(void)fprintf(stderr, "usage: %s -d dbname [-t tabname] [-e] [filename]\n", argv[0]);
		else
			(void)fprintf(stderr, "usage: %s -d dbname [-t tabname] [-s [-l]] [-e] [filename]\n", argv[0]);
		exit(1);
	}

	/* locate the database in the system */
	sprintf(exec_stmt, "database %s", database);
	$prepare db_exec from $exec_stmt;
	$execute db_exec;
	if (sqlca.sqlcode != SUCCESS) {
		fprintf(stderr, "Database not found or no system permission.\n\n");
		exit(1);
	}

	/* build the select statement */
	if (tflg) {
		if (strchr(table, '*') == NULL &&
		    strchr(table, '[') == NULL &&
		    strchr(table, '?') == NULL)
			sprintf(qry_stmt, "select tabname, tabid from systables where tabname = \"%s\" and tabtype = \"T\"", table);
		else
			sprintf(qry_stmt, "select tabname, tabid from systables where tabname matches \"%s\" and tabtype = \"T\"", table);
	} else
		sprintf(qry_stmt, "select tabname, tabid from systables where tabtype = \"T\" order by tabname");

	/* declare some cursors */
	$prepare tab_query from $qry_stmt;
	$declare tab_cursor cursor for tab_query;
	$declare col_cursor cursor for
	    select colname, colno from syscolumns
	    where tabid = $systables.tabid order by colno;

	if (argc > optind) {
		if ((fp = fopen(argv[argc - 1], "w")) == NULL) {
			fprintf(stderr, "Could not open %s\n", argv[argc - 1]);
			exit(1);
		}
	} else
		fp = stdout;

	/* read the database for the table(s) and create some output */
	$open tab_cursor;
	$fetch tab_cursor into $systables.tabname, $systables.tabid;
	if (sqlca.sqlcode == SQLNOTFOUND)
		fprintf(stderr, "Table %s not found.\n", table);
	while (sqlca.sqlcode == SUCCESS) {
		if (systables.tabid >= 100) {
			build_dirpath();
			rtrim(systables.tabname);
			if (loadflg) {
				if (!eflg)
					fprintf(fp, "LOAD FROM \"%s.unl\" INSERT INTO %s;\n", systables.dirpath, systables.tabname);
				else {
					do_insert();
					if (!tflg)
						putc('\n', fp);
				}
			} else {
				if (sflg) {
					if (lflg)
						fprintf(fp, "BEGIN WORK;\n");
					fprintf(fp, "LOCK TABLE %s IN SHARE MODE;\n", systables.tabname);
				}
				if (!eflg)
					fprintf(fp, "UNLOAD TO \"%s.unl\" SELECT * FROM %s;\n", systables.dirpath, systables.tabname);
				else
					do_select();
				if (sflg) {
					if (lflg)
						fprintf(fp, "COMMIT WORK;\n");
					else
						fprintf(fp, "UNLOCK TABLE %s;\n", systables.tabname);
				}
				if ((eflg || sflg) && !tflg)
					putc('\n', fp);
			}
		}
		$fetch tab_cursor into $systables.tabname, $systables.tabid;
	}
	$close tab_cursor;

	exit(0);
}


/*******************************************************************************
* This function explodes the INSERT statement for "dbloader".                  *
*******************************************************************************/

do_insert() 
{
	register int	i = 0;

	fprintf(fp, "LOAD FROM \"%s.unl\"\n  INSERT INTO %s (\n", systables.dirpath, systables.tabname);
	$open col_cursor;
	$fetch col_cursor into $syscolumns.colname;
	while (sqlca.sqlcode == SUCCESS) {
		rtrim(syscolumns.colname);
		if (i++)
			fprintf(fp, ",\n    %s", syscolumns.colname);
		else
			fprintf(fp, "    %s", syscolumns.colname);
		$fetch col_cursor into $syscolumns.colname;
	}
	$close col_cursor;
	fprintf(fp, "\n  );\n");
}


/*******************************************************************************
* This function explodes the SELECT statement for "dbunloader".                *
*******************************************************************************/

do_select() 
{
	register int	i = 0;

	fprintf(fp, "UNLOAD TO \"%s.unl\"\n  SELECT\n", systables.dirpath);
	$open col_cursor;
	$fetch col_cursor into $syscolumns.colname;
	while (sqlca.sqlcode == SUCCESS) {
		rtrim(syscolumns.colname);
		if (i++)
			fprintf(fp, ",\n      %s", syscolumns.colname);
		else
			fprintf(fp, "      %s", syscolumns.colname);
		$fetch col_cursor into $syscolumns.colname;
	}
	$close col_cursor;
	fprintf(fp, "\n    FROM %s;\n", systables.tabname);
}


/*******************************************************************************
* This function will search s1 for s2 starting at n.  It returns where in s1,  *
* s2 was found or -1.
*******************************************************************************/

strnsch(s1, s2, n)
char	*s1;
char	*s2;
int	n;
{
	int	i;

	for (i = n; i < (strlen(s1) - strlen(s2) + 1); i++)
		if (strncmp(s1 + i, s2, strlen(s2)) == 0)
			return(i);

	return(-1);
}


/*******************************************************************************
* This function will trim trailing spaces from s.                              *
*******************************************************************************/

rtrim(s)
char	*s;
{
	int	i;

	for (i = strlen(s) - 1; i >= 0; i--)
		if (!isgraph(s[i]) || !isascii(s[i]))
			s[i] = '\0';
		else
			break;
}


/*******************************************************************************
* This function will build the dirpath from tabname and tabid.                 *
*******************************************************************************/

build_dirpath()
{
	int	i;

	sprintf(systables.dirpath, "%-7.7s%d", systables.tabname, systables.tabid);

	for (i = 0; i < 10; i++)
		if (systables.dirpath[i] == ' ')
			systables.dirpath[i] = '_';

	sprintf(&systables.dirpath[7 - (strlen(systables.dirpath) - 10)], "%d", systables.tabid);
}


