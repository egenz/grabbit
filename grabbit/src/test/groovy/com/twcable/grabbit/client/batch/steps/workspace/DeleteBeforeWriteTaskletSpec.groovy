package com.twcable.grabbit.client.batch.steps.workspace

import org.apache.sling.jcr.api.SlingRepository
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.repeat.RepeatStatus
import spock.lang.Specification

import javax.jcr.Node
import javax.jcr.PathNotFoundException
import javax.jcr.Session

import static com.twcable.jackalope.JCRBuilder.node
import static com.twcable.jackalope.JCRBuilder.repository

/*
 * Copyright 2015 Time Warner Cable, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@SuppressWarnings("GroovyAccessibility")
class DeleteBeforeWriteTaskletSpec extends Specification {

    def "Exclude paths bean is set correctly"() {
        when:
        final deleteBeforeWriteTasklet = new DeleteBeforeWriteTasklet()
        deleteBeforeWriteTasklet.setExcludePaths(excludePaths)

        then:
        expectedExcludePaths == deleteBeforeWriteTasklet.excludePaths

        where:
        excludePaths        |   expectedExcludePaths
        null                |   []
        "foopath"           |   ["/foopath"]
        "/foopath "         |   ["/foopath"]
        "/foo/bar*/foo/doo" |   ["/foo/bar", "/foo/doo"]
        "foo/bar*foo/doo"   |   ["/foo/bar", "/foo/doo"]
    }


    def "Job path bean is set correctly"() {
        when:
        final deleteBeforeWriteTasklet = new DeleteBeforeWriteTasklet()
        deleteBeforeWriteTasklet.setJobPath(jobPath)

        then:
        expectedJobPath == deleteBeforeWriteTasklet.jobPath

        where:
        jobPath         |   expectedJobPath
        "foo/bar"       |   "/foo/bar"
        "/foo/bar "     |   "/foo/bar"
    }

    def "If no exclude paths, the entire path is deleted"() {
        given:
        final jobPath = "/foo/bar"

        when:
        final session = Mock(Session) {
            getNode(jobPath) >> Mock(Node) {
                1 * remove()
            }
            impersonate(_) >> it
            1 * save()
        }
        final slingRepository = Mock(SlingRepository) {
            loginAdministrative(null) >> session
        }

        final deleteBeforeWriteTasklet = new DeleteBeforeWriteTasklet()
        deleteBeforeWriteTasklet.setSlingRepository(slingRepository)
        deleteBeforeWriteTasklet.setJobPath(jobPath)

        then:
        RepeatStatus.FINISHED == deleteBeforeWriteTasklet.execute(Mock(StepContribution), Mock(ChunkContext))
    }

    def "If exclude paths are provided, we don't delete those paths"() {
        given:
        final repository = repository(
                node("root",
                    node("a",
                        node("a",
                            node("a"),
                            node("b"),
                            node("c")
                        ),
                        node("b")
                    ),
                    node("b",
                        node("a"),
                        node("b")
                    ),
                    node("c")
                )
        ).build()

        final deleteBeforeWriteTasklet = new DeleteBeforeWriteTasklet()
        deleteBeforeWriteTasklet.setSlingRepository(repository)
        final excludePaths = ["/root/a/a/b", "/root/a/a/c", "/root/b"]
        deleteBeforeWriteTasklet.setExcludePaths(excludePaths.join('*'))
        deleteBeforeWriteTasklet.setJobPath("/root")

        when:
        deleteBeforeWriteTasklet.execute(Mock(StepContribution), Mock(ChunkContext))

        final session = repository.login()
        final excludedPathsNotDeleted = excludePaths.every {
            try {
                session.getNode(it)
                return true
            }
            catch(PathNotFoundException ex) {
                return false
            }
        }

        then:
        excludedPathsNotDeleted
    }
}
