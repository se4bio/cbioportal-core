/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.mskcc.cbio.portal.integrationTest.scripts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cbio.portal.dao.DaoCancerStudy;
import org.mskcc.cbio.portal.dao.DaoException;
import org.mskcc.cbio.portal.dao.DaoGeneticAlteration;
import org.mskcc.cbio.portal.dao.DaoGeneticProfile;
import org.mskcc.cbio.portal.dao.DaoGeneticProfileSamples;
import org.mskcc.cbio.portal.dao.DaoSample;
import org.mskcc.cbio.portal.model.CancerStudy;
import org.mskcc.cbio.portal.model.GeneticProfile;
import org.mskcc.cbio.portal.scripts.RemoveSamples;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * JUnit tests for RemoveSamples class.
 *
 * @author Ruslan Forostianov
 * @author Pieter Lukasse
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:/applicationContext-dao.xml"})
@Rollback
@Transactional
public class TestRemoveSamples {

    @Test
    public void testRemoveSamples() throws DaoException {
        String sample1StableId = "TCGA-A1-A0SB-01";
        String sample2StableId = "TCGA-A1-A0SD-01";
        CancerStudy cancerStudy = DaoCancerStudy.getCancerStudyByStableId("study_tcga_pub");
        List<String> beforeSampleIds = DaoSample.getSampleStableIdsByCancerStudy(cancerStudy.getInternalId());
        assertTrue(beforeSampleIds.contains(sample1StableId));
        assertTrue(beforeSampleIds.contains(sample2StableId));
        int sample1InternalId = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), sample1StableId).getInternalId();
        int sample2InternalId = DaoSample.getSampleByCancerStudyAndSampleId(cancerStudy.getInternalId(), sample2StableId).getInternalId();

        new RemoveSamples(new String[]{
                "--study_ids", "study_tcga_pub",
                "--sample_ids", "TCGA-A1-A0SB-01,TCGA-A1-A0SD-01"
        }).run();

        DaoSample.reCache();

        List<String> afterSampleIds = DaoSample.getSampleStableIdsByCancerStudy(cancerStudy.getInternalId());
        assertFalse(afterSampleIds.contains(sample1StableId));
        assertFalse(afterSampleIds.contains(sample2StableId));
        assertEquals(beforeSampleIds.size() - 2, afterSampleIds.size());

        List<GeneticProfile> geneticProfiles = Stream.of("study_tcga_pub_gistic", "study_tcga_pub_mrna", "study_tcga_pub_log2CNA",
                "study_tcga_pub_rppa", "study_tcga_pub_treatment_ic50").map(DaoGeneticProfile::getGeneticProfileByStableId).toList();
        for (GeneticProfile geneticProfile : geneticProfiles) {
            HashMap<Integer, HashMap<Integer, String>> geneticAlterationMapForEntityIds = DaoGeneticAlteration.getInstance()
                    .getGeneticAlterationMapForEntityIds(geneticProfile.getGeneticProfileId(), null);
            for (Map.Entry<Integer, HashMap<Integer, String>> gaEntry : geneticAlterationMapForEntityIds.entrySet()) {
                assertFalse(gaEntry.getValue().containsKey(sample1InternalId),
                        "Genetic entity with id "
                                + gaEntry.getKey()
                                + " of " + geneticProfile.getStableId() + "genetic profile"
                                + " must have " + sample1StableId + " sample deleted");
                assertFalse(gaEntry.getValue().containsKey(sample2InternalId),
                        "Genetic entity with id "
                                + gaEntry.getKey()
                                + " of " + geneticProfile.getStableId() + "genetic profile"
                                + " must have " + sample2StableId + " sample deleted");
            }
        }
        int studyTcgaPubMethylationHm27 = DaoGeneticProfile.getGeneticProfileByStableId("study_tcga_pub_methylation_hm27").getGeneticProfileId();
        assertTrue("The methylation platform has to loose it's last sample", DaoGeneticProfileSamples.getOrderedSampleList(
                studyTcgaPubMethylationHm27).isEmpty());
    }
}
