# {"data":{"data":{"$recipeTypes":["com.linkedin.6c74ef9c1fa40b2ad676693e065aca2c"],"identityDashProfilesByMemberIdentity":{"*elements":["urn:li:fsd_profile:ACoAAGTKMocBYOQU2u2bRFdeqpSxZh5zS3X4Mwg"],"$recipeTypes":["com.linkedin.bb1bbd4b03afda37bcfe63d7d41d6132"],"$type":"com.linkedin.restli.common.CollectionResponse"},"$type":"com.linkedin.6c74ef9c1fa40b2ad676693e065aca2c"},"extensions":{"webMetadata":{}}},"meta":{"microSchema":{"isGraphQL":true,"version":"2.1","types":{"com.linkedin.6c74ef9c1fa40b2ad676693e065aca2c":{"fields":{"identityDashProfilesByMemberIdentity":{"type":"com.linkedin.bb1bbd4b03afda37bcfe63d7d41d6132"}},"baseType":"com.linkedin.graphql.Query"},"com.linkedin.bb1bbd4b03afda37bcfe63d7d41d6132":{"fields":{"elements":{"type":{"array":"com.linkedin.ed5d81650dd52f9bb922d19101da543b"}}},"baseType":"com.linkedin.restli.common.CollectionResponse"},"com.linkedin.ed5d81650dd52f9bb922d19101da543b":{"fields":{"entityUrn":{"type":"com.linkedin.voyager.dash.common.ProfileUrn"},"versionTag":{"type":"string"}},"baseType":"com.linkedin.voyager.dash.identity.profile.Profile"}}}},"included":[{"versionTag":"1095106680","entityUrn":"urn:li:fsd_profile:ACoAAGTKMocBYOQU2u2bRFdeqpSxZh5zS3X4Mwg","$recipeTypes":["com.linkedin.ed5d81650dd52f9bb922d19101da543b"],"$type":"com.linkedin.voyager.dash.identity.profile.Profile"}]}
# https://www.linkedin.com/voyager/api/graphql?includeWebMetadata=true&variables=(memberIdentity:ACoAAGTKMocBYOQU2u2bRFdeqpSxZh5zS3X4Mwg)&queryId=voyagerIdentityDashProfiles.b5c27c04968c409fc0ed3546575b9b7a


# https://www.linkedin.com/flagship-web/in/pdh0128/details/skills
# https://www.linkedin.com/flagship-web/in/pdh0128/details/honors/
# sdui 기반

# POST https://www.linkedin.com/flagship-web/in/pdh0128/details/certifications/

# POST https://www.linkedin.com/in/pdh0128/details/education/
# POST https://www.linkedin.com/in/pdh0128/details/experience/
# POST https://www.linkedin.com/in/winshine0326/details/test-scores/
# winshine0326
# 보유 기술
# 수상 경력
# 자격증
# 학력
# 경력
# 시험 점수

# {$type: "proto.sdui.actions.core.NavigateToScreen",…}
# $type
# :
# "proto.sdui.actions.core.NavigateToScreen"
# colorScheme
# :
# "ColorScheme_UNKNOWN"
# disableScreenGutters
# :
# false
# inheritActor
# :
# false
# newHierarchy
# :
# {$type: "proto.sdui.navigation.ScreenHierarchy",…}
# pageKey
# :
# "profile_view_base_honors_details"
# presentation
# :
# {$case: "fullPage", fullPage: {$type: "proto.sdui.actions.core.presentation.FullPagePresentation"}}
# presentationStyle
# :
# "PresentationStyle_FULL_PAGE"
# replaceCurrentScreen
# :
# false
# requestedArguments
# :
# {payload: {vanityName: "pdh0128"}, states: [],…}
# screenId
# :
# "com.linkedin.sdui.flagshipnav.profile.ProfileHonorDetails"
# screenTitle
# :
# ""
# shouldHideLoadingSpinner
# :
# false
# shouldHideMobileTopNavBar
# :
# false
# shouldHideMobileTopNavBarDivider
# :
# false
# title
# :
# ""
# url
# :
# "/in/pdh0128/details/honors/"
