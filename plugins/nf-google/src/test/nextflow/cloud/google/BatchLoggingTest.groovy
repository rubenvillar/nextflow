/*
 * Copyright 2020-2022, Seqera Labs
 * Copyright 2013-2019, Centre for Genomic Regulation (CRG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package nextflow.cloud.google

import java.util.concurrent.TimeUnit

import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.cloud.google.batch.client.BatchClient
import nextflow.cloud.google.batch.client.BatchConfig
import nextflow.cloud.google.batch.logging.BatchLogging
import nextflow.cloud.google.batch.model.BatchJob
import spock.lang.IgnoreIf
import spock.lang.Requires
import spock.lang.Specification
import spock.lang.Timeout
/**
 *
 * @author Paolo Di Tommaso <paolo.ditommaso@gmail.com>
 */
@Slf4j
class BatchLoggingTest extends Specification {

    def 'should parse stdout and stderr' () {
        given:
        def OUT_TEXT = '  Task action/STARTUP/0/0/group0/0, STDOUT:  No user sessions are running outdated binaries.\n'
        def ERR_TEXT = '  Task action/STARTUP/0/0/group0/0, STDERR:  Oops something has failed. We are sorry.\n'
        and:
        def client = new BatchLogging()

        when:
        def stdout = new StringBuilder();
        def stderr = new StringBuilder()
        and:
        client.parseOutput(OUT_TEXT, stdout, stderr)
        then:
        stdout.toString() == 'No user sessions are running outdated binaries.\n'
        and:
        stderr.toString() == ''
        and:
        client.currentMode() == 'STDOUT:  '

        when:
        client.parseOutput(ERR_TEXT, stdout, stderr)
        then:
        stderr.toString() == 'Oops something has failed. We are sorry.\n'
        and:
        client.currentMode() == 'STDERR:  '

        when:
        client.parseOutput('blah blah', stdout, stderr)
        then:
        // the message is appended to the stderr because not prefix is provided
        stderr.toString() == 'Oops something has failed. We are sorry.\nblah blah'
        and:
        // no change to the stdout
        stdout.toString() == 'No user sessions are running outdated binaries.\n'
        and:
        client.currentMode() == 'STDERR:  '

        when:
        client.parseOutput('STDOUT:  Hello world', stdout, stderr)
        then:
        // the message is added to the stdout
        stdout.toString() == 'No user sessions are running outdated binaries.\nHello world'
        and:
        // no change to the stderr
        stderr.toString() == 'Oops something has failed. We are sorry.\nblah blah'
        and:
        client.currentMode() == 'STDOUT:  '

    }

    @Timeout(value = 10, unit = TimeUnit.MINUTES)
    @IgnoreIf({System.getenv('NXF_SMOKE')})
    @Requires({System.getenv('GOOGLE_APPLICATION_CREDENTIALS')})
    def 'should fetch logs' () {
        given:
        def sess = Mock(Session) { getConfig() >> [:] }
        def config = BatchConfig.create(sess)
        and:
        def batchClient = new BatchClient(config)
        def logClient = new BatchLogging(config)

        when:
        def cmd = ['-c','echo Hello world! && echo "Oops something went wrong" >&2']
        def req = BatchJob.create(imageUri: 'quay.io/nextflow/bash', command: cmd)
        def jobId = 'nf-test-' + System.currentTimeMillis()
        def resp = batchClient.submitJob(jobId, req)
        def uid = resp.get('uid') as String
        log.debug "Test job uid=$uid"
        then:
        uid
        
        when:
        def state=null
        do {
            state = batchClient.getJobState(jobId)
            log.debug "Test job state=$state"
            sleep 10_000
        } while( state !in ['SUCCEEDED', 'FAILED'] )
        then:
        state in ['SUCCEEDED', 'FAILED']

        when:
        def stdout = logClient.stdout(uid)
        def stderr = logClient.stderr(uid)
        log.debug "STDOUT: $stdout"
        log.debug "STDERR: $stderr"
        then:
        stdout == 'Hello world!\n'
        stderr.contains('Oops something went wrong')

    }
}
