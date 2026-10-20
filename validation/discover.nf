#!/usr/bin/env nextflow
nextflow.enable.dsl=2

include { publishEvent} from 'plugin/nf-ws-events'

/*
 *  validation/main.nf
 *
 *  A tiny but illustrative pipeline that:
 *
 *    1. Runs a discovery step.
 *    2. Splits that discovery into N sub‑tasks (parallelization).
 *    3. Runs a final summariser that consumes all sub‑task outputs.
 *
 *  It is deliberately VERY small – the only goal is to exercise the
 *  `wsEvents` plugin with many process start/complete events.
 */

process DISCOVER {
    tag "Discover"

    output:
    val('1,2,3,4,5'), emit: ids

    script:
    """
    echo 'Discovering IDs...'
    """
}

process SPLIT {
    input:
    val ids

    output:
    val(ids),emit: id

    script:
    """
    echo ${ids} | tr ',' '\n' | shuf | head -n 1
    """

}

process ANALYSE {
    tag "$id"

    input:
    val id

    output:
    val id, emit: id
    stdout emit: result

    script:
    """
    echo "metric_${id}"
    """
}

process SUMMARISE {
    tag "$id"

    input:
    val id
    val(result)

    script:
    """
    echo "Result for ${id}: ${result}"
    """
}

workflow {
    // 1️⃣  discovery step
    DISCOVER()          // → emits `ids`
    
    // 2️⃣  split it into N parallel sub‑tasks
    //LIT(DISCOVER.out.ids)     // → emits many `id`s
    ids_ch = DISCOVER.out.ids
        .map { it.trim().split(',') }
        .flatMap { it }

    // 3️⃣  analyse everyone
    ANALYSE(ids_ch)   // → emits (`id`, `result`)

    // collect all ANALYSE outputs
    def all = ANALYSE.out.result.collect()

    all.subscribe { results ->
        publishEvent("Analyse", "Completed", null)
    }

    // 4️⃣  final step that consumes all the results
    SUMMARISE(
        ANALYSE.out.id,
        ANALYSE.out.result
    )
}