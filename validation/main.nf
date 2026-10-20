nextflow.enable.dsl=2

process STEP_1 {
    output:
    val 'one', emit: result

    script:
    '''
    echo step-one
    '''
}

process STEP_2 {
    input:
    val previous

    output:
    val 'two', emit: result

    script:
    """
    echo got-${previous}
    """
}

workflow {
    STEP_1()
    STEP_2(STEP_1.out.result)
}
