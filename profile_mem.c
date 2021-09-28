#include "aqo.h"
#include "profile_mem.h"

#include "funcapi.h"
#include "miscadmin.h"

int 	aqo_profile_mem;
bool	out_of_memory = false;
int i = 0;
static HTAB   *profile_mem_queries = NULL;

typedef struct ProfileMemEntry
{
	int key;
	double time;
} ProfileMemEntry;

PG_FUNCTION_INFO_V1(aqo_profile_mem_hash);

static bool init_profile_shmem(void);

Datum
aqo_profile_mem_hash(PG_FUNCTION_ARGS)
{
	HASH_SEQ_STATUS hash_seq;
	ProfileMemEntry *entry;
    TupleDesc tupdesc;
	HeapTuple tuple;
    AttInMetadata *attinmeta;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	Tuplestorestate *tupstore;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not allowed in this context")));

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	attinmeta = TupleDescGetAttInMetadata(tupdesc);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (!init_profile_shmem())
	{
		ReleaseTupleDesc(tupdesc);
		tuplestore_donestoring(tupstore);
		elog(WARNING, "Hash table 'profile_mem_queries' doesn't exist");
		PG_RETURN_VOID();
	}

	hash_seq_init(&hash_seq, profile_mem_queries);
	while (((entry = (ProfileMemEntry *) hash_seq_search(&hash_seq)) != NULL))
	{
		char **values;

		values = (char **) palloc(2 * sizeof(char *));
		values[0] = (char *) palloc(16 * sizeof(char));
		values[1] = (char *) palloc(16 * sizeof(char));

		snprintf(values[0], 16, "%d", entry->key);
		snprintf(values[1], 16, "%0.5f", entry->time);

		tuple = BuildTupleFromCStrings(attinmeta, values);
		tuplestore_puttuple(tupstore, tuple);
	}

	ReleaseTupleDesc(tupdesc);
	tuplestore_donestoring(tupstore);

	PG_RETURN_VOID();
}

/*
 * Allocate and initialize profiling-related shared memory, if not already
 * done, and set up backend-local pointer to that state.  Returns false if this
 * operation was failed.
 */
static bool
init_profile_shmem(void)
{
	if (profile_mem_queries)
		return true;

	if (aqo_profile_mem > 0)
	{
		HASHCTL hash_ctl;
		int mem_size = (int) (aqo_profile_mem * 1048576 / sizeof(ProfileMemEntry));

		hash_ctl.keysize = sizeof(int);
		hash_ctl.entrysize = sizeof(ProfileMemEntry);
		elog(WARNING, "aqo_profile_mem2: %d - %d", aqo_profile_mem, mem_size);
		profile_mem_queries = ShmemInitHash("aqo_profile_mem_queries",
											mem_size,
											mem_size,
											&hash_ctl,
											HASH_ELEM | HASH_BLOBS);
		elog(WARNING, "aqo_profile_mem1: %d", aqo_profile_mem);
	}
	return (profile_mem_queries != NULL);
}

void
set_profile_mem(int newval, void *extra)
{
	if (newval <= 0)
	{
		aqo_profile_mem = -1;
		return;
	}

	aqo_profile_mem = newval * 1048576; /* calculate size in bytes */
	elog(WARNING, "Initial shared memory size for AQO profiling hash-table: %d (bytes).", aqo_profile_mem);
}

void
update_profile_mem_table(void)
{
	bool found;
    ProfileMemEntry *pentry;
    double totaltime;
    instr_time endtime;

	if (!init_profile_shmem())
	{
		elog(LOG, "Something went wrong during initialization of an AQO profiling hash table. Disable this feature for the backend.");
		aqo_profile_mem = -1;
	}

    if (aqo_profile_mem > 0)
	{
		INSTR_TIME_SET_CURRENT(endtime);
		INSTR_TIME_SUBTRACT(endtime, query_context.query_starttime);
		totaltime = INSTR_TIME_GET_DOUBLE(endtime);

        PG_TRY();
		{
			if (!out_of_memory)
			{
				pentry = (ProfileMemEntry *) hash_search(profile_mem_queries, &query_context.query_hash, HASH_ENTER, &found);
				if (!found)
				{
					elog(WARNING, "Q %d not found!", query_context.query_hash);
					pentry->time = 0;
				}
				pentry->time += totaltime - query_context.query_planning_time;
			}
		}
 		PG_CATCH();
		{
			elog(LOG, "Failed to change aqo_profile_mem_queries table.");
			out_of_memory = true;
		}
 		PG_END_TRY();
	}
}